import datasets, re
from mysql.connector import Error
from mysql.connector.pooling import MySQLConnectionPool
test_counter = 0
with open("Qwen1.5/test-output.txt", "w") as f:
    pass

# Establish connection pool
pool = MySQLConnectionPool(
    pool_name="mypool",
    pool_size=5,
    host="rm-8vbc9w1qz2f0d533ulo.mysql.zhangbei.rds.aliyuncs.com",
    port="3306",
    user="ai_sgcc",
    password="Gaimima123",
    database="text-to-sql-data"
)
def get_connection():
    return pool.get_connection()


# for lm-evaluation-harness
def process_docs(dataset: datasets.Dataset):
    def _helper(doc):
        # modifies the contents of a single
        # document in our dataset.
        return doc

    return dataset.map(_helper) # returns back a datasets.Dataset object

def evaluator_wrapper(references, predictions):
    global test_counter
    test_counter += 1

    # save
    output_path = "Qwen1.5/test-output.txt"
    with open(output_path, "a") as f:
        f.write(f"***Counter: {test_counter}\n###Golden: {references[0]}\n###Prediction: {predictions[0]}\n\n")

    # evaluate
    return evaluator(references[0], predictions[0])


# exec accuracy evaluation
def evaluator(query1, query2):
    '''比较两个查询的结果是否相同'''
    results1 = connect_execute_query(query1)
    results2 = connect_execute_query(query2)

    if (len(results1) == 2 and results1[0] == "Error") or (len(results2) == 2 and results2[0] == "Error"):
        return 0
    
    if not results1 or not results2:
        if not results1 and not results2:
            return 1
        return 0

    sorted_results1 = sorted([tuple(sorted(str(item) for item in row)) for row in results1])
    sorted_results2 = sorted([tuple(sorted(str(item) for item in row)) for row in results2])

    return int(sorted_results1 == sorted_results2)

def connect_execute_query(query):
    '''连接数据库并执行查询'''
    connection = None
    cursor = None
    try:
        connection = get_connection()

        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            return result

    except Error as e:
        error_message = f"Database error: {e}"
        return "Error", error_message

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()

# extractor
def extract_first_select_statement(text):
    """
    从文本中提取第一个 SELECT SQL 语句。

    参数:
    text (str): 输入文本，可能包含多个 SQL 语句。
    
    返回:
    str: 第一个出现的 SELECT SQL 语句，或 None 如果没有找到。
    
    示例:
    >>> extract_first_select_statement("some text\\nSELECT column1 FROM table;")
    'SELECT column1 FROM table'
    """
    match = re.search(r"(SELECT|select)(.|\n)*?(;|$)", text)
    if match:
        return match.group(0).rstrip(';')
    else:
        return "NO SQL"
