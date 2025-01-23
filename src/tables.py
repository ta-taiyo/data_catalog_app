# 必要なライブラリをインポート
import snowflake.snowpark.functions as F
import pandas as pd

def get_table_comment(tablename, session):
    """テーブルの現在のコメントを取得する関数"""
    
    # テーブル名をスキーマ名とテーブル名に分割
    tbl_context = tablename.split('.')
    tbl, schema = tbl_context[-1], '.'.join(tbl_context[:-1])
    # SQLを実行してテーブルコメントを取得し、シングルクォートをエスケープ
    return session.sql(f"SHOW TABLES LIKE '{tbl}' IN SCHEMA {schema} LIMIT 1").collect()[0]['comment']\
                  .replace("'", "\\'")

def convert_vec2array(tablename, session):
    """ベクトル型のカラムを配列型に変換する関数"""

    # Snowflakeの型定義をインポート
    import snowflake.snowpark.types as T

    # テーブルをデータフレームとして読み込み
    df = session.table(tablename)
    # ベクトル型のカラムを特定
    vec_cols = [c.name for c in df.schema.fields if (type(c.datatype) == T.VectorType)]
    if vec_cols:
        # ベクトル型カラムを配列に変換（最初の10要素のみ）
        return df.select([F.array_slice(F.to_array(x), F.lit(0), F.lit(10)).as_(x) if x in vec_cols else x for x in df.columns])
    else:
        # ベクトル型カラムがない場合は元のデータフレームを返す
        return df
    
def pctg_nonnulls(df):
    """データフレームの各行における非NULL値の割合を計算する関数"""

    import pandas
    from _snowflake import vectorized
    # NULL値や空文字の割合を計算して1から引く
    return 1 - sum(el in [None, ''] for el in df)/len(df)

def sample_tbl(tablename, sampling_mode, n, session):
    """指定されたサンプリング方法でテーブルからn件のサンプルを取得する関数"""

    # ウィンドウ関数をインポート
    from snowflake.snowpark.window import Window

    # ベクトル型を配列に変換
    df = convert_vec2array(tablename, session)
    if sampling_mode == "fast":
        # ランダムサンプリング
        samples = df.sample(n = n)\
            .select(F.to_varchar(F.array_agg(F.object_construct('*'))))\
            .to_pandas().values[0][0]
    elif sampling_mode == 'nonnull':
        # NULL値が最も少ない順に並べ替えて最初のn件を取得
        samples = df.withColumn('ROWNUMBER', 
                            F.row_number().over(Window.partition_by().order_by(F.desc(F.call_udf('PCTG_NONNULL', F.array_construct('*'))))))\
                .sort(F.col('ROWNUMBER')).drop(F.col('ROW_NUMBER'))\
                .select(F.to_varchar(F.array_slice(F.array_agg(F.object_construct('*')), F.lit(0), F.lit(n))))\
                .to_pandas().values[0][0]
    else:
        # 無効なサンプリングモードの場合はエラーを発生
        raise ValueError("sampling_mode must be one of ['fast' (Default), 'nonnull'].") 
    # シングルクォートをエスケープして返す
    return samples.replace("'", "\\'")

def cortex_sql(session, model, prompt, temperature):
    """SQL経由でCORTEX COMPLETEを実行する関数
    
    temperatureパラメータが必要な場合に使用（Python APIではtemperatureがサポートされていない）
    """
    # SQLクエリを構築
    query = f"""
    SELECT TRIM(SNOWFLAKE.CORTEX.COMPLETE(
    '{model}', 
    [
        {{
            'role': 'user',
            'content': '{prompt}'
        }}
    ],
    {{
        'temperature': {temperature}
    }}
    ):choices[0]:messages) AS RESPONSE
    """
    # クエリを実行して結果を返す
    result = session.sql(query).collect()[0][0]
    return result

def run_complete(session, tablename, model, sampling_mode, n, prompt, temperature = None):
    """テーブルの最も空でないサンプルレコードを基にLLMによる説明を生成する関数"""

    # 必要なライブラリをインポート
    import textwrap
    from snowflake.cortex import Complete
    from snowflake.snowpark.exceptions import SnowparkSQLException

    # サンプルデータを取得
    samples = sample_tbl(tablename, sampling_mode, n, session)

    try:
        # プロンプトの中の波括弧をエスケープしてSQL変換エラーを防ぐ
        prompt = textwrap.dedent(prompt.format(table_samples = samples))
        print(prompt)
        # temperatureが有効な値の場合はSQL経由で実行
        if isinstance(temperature, float):
            if temperature > 0 and temperature < 1: 
                response = cortex_sql(session,
                                    model,
                                    prompt,
                                    temperature)
            else:
                # 無効なtemperatureの場合はデフォルト値を使用
                response = Complete(model, 
                                    prompt,
                                    session = session)
        else:
            # temperatureが指定されていない場合は通常のAPI経由で実行
            response = Complete(model, 
                                prompt,
                                session = session)
        # レスポンスを整形してシングルクォートをエスケープ
        response = str(response).strip().replace("'", "\\'")
        
        return ("success", response)
    except SnowparkSQLException as e:
        # トークン制限エラーの場合
        if 'max tokens' in str(e):
            raise NotImplementedError(f"{e}.\nCortex token counter will be added once available. Try a different model or fewer sample rows.")
        else:
            return ("fail", f"""LLM-generation Error Encountered: {e}""")
    except Exception as e:
        # その他のエラーの場合
        return ("fail", f"""LLM-generation Error Encountered: {e}""")
    
def get_crawlable_tbls(session,
                     database,
                     schema,
                     catalog_database,
                     catalog_schema,
                     catalog_table,
                     ignore_catalog = False):
    """カタログ化されていないデータベース/スキーマ内のテーブル一覧を取得する関数"""

    # スキーマの条件を設定
    if schema:
        schema_qualifier = f"= '{schema}'"
    else:
        schema_qualifier = "<> 'INFORMATION_SCHEMA'"

    # カタログの制約を設定
    if ignore_catalog:
        catalog_constraint = ""
    else:
        catalog_constraint = f"""NATURAL FULL OUTER JOIN {catalog_database}.{catalog_schema}.{catalog_table}
                                 WHERE {catalog_database}.{catalog_schema}.{catalog_table}.TABLENAME IS NULL"""
    
    # クエリを構築
    query = f"""
    WITH T AS (
        SELECT 
            TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS TABLENAME
            FROM {database}.INFORMATION_SCHEMA.tables 
            WHERE TABLE_SCHEMA {schema_qualifier}
            AND (ROW_COUNT >= 1 OR ROW_COUNT IS NULL)
            AND IS_TEMPORARY = 'NO'
            AND NOT STARTSWITH(TABLE_NAME, '_')
            )
    SELECT 
        T.TABLENAME
    FROM T 
    {catalog_constraint}
    """
    # クエリを実行してテーブル名のリストを返す
    return session.sql(query).to_pandas()['TABLENAME'].values.tolist()

def get_unique_context(tablenames):
    """クロール用のターゲットデータベースとスキーマ名の一意セットを取得する関数"""
    # スキーマ名の一意セットを作成
    schemas = {".".join(t.split(".")[:-1]) for t in tablenames}
    # データベース名を取得
    db = tablenames[0].split('.')[0]
    return db, schemas

def get_all_tables(session, target_database, target_schemas):
    """[スキーマ, テーブル, テーブルコメント, カラム情報]を含むデータフレームを返す関数"""
    # スキーマ名をカンマ区切りの文字列に変換
    target_schema_str = ','.join(f"'{t.split('.')[1]}'" for t in target_schemas)
    # クエリを構築
    query = f"""
        WITH T AS 
    (
        SELECT 
            TABLE_SCHEMA
            , TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS TABLENAME
            ,REGEXP_REPLACE(COMMENT, '{{|}}','') AS TABLE_COMMENT
        FROM {target_database}.INFORMATION_SCHEMA.tables 
        WHERE 1=1 
            AND TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
            AND TABLE_SCHEMA IN ({target_schema_str})
            AND (ROW_COUNT >= 1 OR ROW_COUNT IS NULL)
            AND IS_TEMPORARY = 'NO'
            AND NOT STARTSWITH(TABLE_NAME, '_')
        )
    , C AS (
        SELECT
            TABLE_SCHEMA
            ,TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS TABLENAME
            ,LISTAGG(CONCAT(COLUMN_NAME, ' ', DATA_TYPE, COALESCE(concat(' (', REGEXP_REPLACE(COMMENT, '{{|}}',''), ')'), '')), ', ') as COLUMN_INFO
        FROM {target_database}.INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1 
            AND TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
            AND TABLE_SCHEMA IN ({target_schema_str})
            AND NOT STARTSWITH(TABLE_NAME, '_')
        GROUP BY TABLE_SCHEMA, TABLENAME
        )
    SELECT
        *
        , 'Table: ' || TABLENAME || ', Comment: ' || COALESCE(TABLE_COMMENT, 'No comment') || ', Columns: ' || COLUMN_INFO AS TABLE_DDL
    FROM T NATURAL INNER JOIN C
    """
    # クエリを実行してデータフレームを返す
    return session.sql(query).to_pandas()

def add_records_to_catalog(session,
                           catalog_database,
                           catalog_schema,
                           catalog_table,
                           new_df,
                           replace_catalog = True):
    """カタログにレコードを追加または更新する関数"""
    
    if replace_catalog:
        # 既存のカタログを読み込み
        current_df = session.table(f'{catalog_database}.{catalog_schema}.{catalog_table}')
        # マージ操作を実行
        _ = current_df.merge(new_df, current_df['TABLENAME'] == new_df['TABLENAME'],
                 [F.when_matched().update({'DESCRIPTION': new_df['DESCRIPTION'],
                                           'CREATED_ON': new_df['CREATED_ON'],
                                           'EMBEDDINGS': F.call_udf('SNOWFLAKE.CORTEX.EMBED_TEXT_1024',
                                                                   'voyage-multilingual-2',
                                                                    new_df['DESCRIPTION'])}),
                  F.when_not_matched().insert({'TABLENAME': new_df['TABLENAME'],
                                               'DESCRIPTION': new_df['DESCRIPTION'],
                                               'CREATED_ON': new_df['CREATED_ON'],
                                               'EMBEDDINGS': F.call_udf('SNOWFLAKE.CORTEX.EMBED_TEXT_1024',
                                                                   'voyage-multilingual-2',
                                                                    new_df['DESCRIPTION'])})])
    else:
        # 新しいレコードを追加
        new_df.write.save_as_table(table_name = [catalog_database, catalog_schema, catalog_table],
                                mode = "append",
                                column_order = "name")

def generate_description(session,
                         tablename,
                         prompt,
                         sampling_mode,
                         n,
                         model,
                         update_comment
                         ):
    """Snowflakeのテーブルオブジェクトをカタログ化する関数"""
    
    from snowflake.snowpark.exceptions import SnowparkSQLException

    # 初期化
    response = ''
    try:
        # LLMを使用して説明を生成
        ctx_response, response = run_complete(session,
                                              tablename,
                                              model, 
                                              sampling_mode,
                                              n,
                                              prompt)
        # コメントの更新が要求され、生成が成功した場合
        if update_comment and ctx_response == 'success':
            try:
                # テーブルのコメントを更新
                session.sql(f"COMMENT IF EXISTS ON TABLE {tablename} IS '{response}'").collect()
            except SnowparkSQLException as e:
                try:
                    # テーブルがビューの場合
                    session.sql(f"COMMENT IF EXISTS ON VIEW {tablename} IS '{response}'").collect()
                except Exception as e:
                    response = f'Error encountered: {str(e)}'
            except Exception as e:
                response = f'Error encountered: {str(e)}'
    except Exception as e:
        response = f'Error encountered: {str(e)}'
    # 結果を辞書形式で返す
    return {
        'TABLENAME': tablename,
        'DESCRIPTION': response.replace("\\", "")
        }
