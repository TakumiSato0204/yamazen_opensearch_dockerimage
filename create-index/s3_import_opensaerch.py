import boto3
import requests
import bs4
from requests_aws4auth import AWS4Auth
from botocore.exceptions import ClientError


# リージョン
region = 'ap-northeast-1' # e.g. us-west-1

# サービス名(Amazon OpenSearch Serverless)
service = 'aoss'

# セッションから認証情報を取得
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# ホスト名
host = ''

# インデックス名
index = 'genbato-index'

# データタイプ(インデックス作成は_doc)
datatype = '_doc'

headers = { "Content-Type": "application/json" }
s3 = boto3.client('s3')

# parameter storeのopensearch-hostのPATH
ssm_parameter_key = {
    "auth_key" : "%2Flambda%2Fopensearch-host"
}

# parameter storeのhost-nameのPATH
ssm_host_parameter_key = {
    "auth_key" : "%2Flambda%2Fcreate-index%2Fhost-name"
}

# htmlからデータを切り出す関数
def create_index(soup,key):

    # タイトルを取得
    title_tag = soup.find('index-title')
    title = title_tag.text if title_tag else ""

    # class="title"を持つ要素を除外
    for title_element in soup.find_all(class_='title'):
        title_element.extract()

    #categoryLを取得
    categoryL = []
    categoryM = []
    categoryS = []
    categoryL_tag = soup.find(class_='entrycategory')
    if categoryL_tag:
        lines = categoryL_tag.get_text().split('\n')
        # 空の行を削除
        lines = [line.strip() for line in lines if line.strip() != '']

        
        if len(lines) == 4:
            categoryL.append(lines[1])
            categoryM.append(lines[2])
            categoryS.append(lines[3])
        elif len(lines) == 3:
            categoryL.append(lines[1])
            categoryM.append(lines[2])
        elif len(lines) == 2:
            categoryL.append(lines[1])
    
    other_categoryL_tag = soup.find(class_='othrentrycategory')
    if other_categoryL_tag:
        other_lines = other_categoryL_tag.get_text().split('\n')
        # 空の行を削除
        other_lines = [line.strip() for line in other_lines if line.strip() != '']
        for line in other_lines:
            categoryS.append(line)

    # <div class="container-fluid"> タグ内のテキストを抽出
    content = ''
    div_main = soup.find(class_='index-content')
    
    if div_main:
        content = div_main.get_text()

        # 空白文字を削除し、テキストを一行に結合
        content = ' '.join(content.split())

    host_url= get_host_url()

    link = host_url + key

    document = { "title": title,"content": content,"link":link ,"categoryL":categoryL,"categoryM":categoryM,"categoryS":categoryS}

    return document



# Get ssm parameter
def get_ssm_parameter(ssm_parameter_path,retry_count):
    """パラメータストアから渡されたkeyの値を取得する関数

    Args:
        ssm_parameter_path (Str): パラメータストアのkey
        retry_count (int): リトライ回数

    Raises:
        Exception: _description_

    Returns:
        Str: パラメータストアに登録してある値
    """
    
    end_point = 'http://localhost:2773'
    path = '/systemsmanager/parameters/get/?name=' + ssm_parameter_path
    parameter_store_url = end_point + path
    headers = {
        'X-Aws-Parameters-Secrets-Token': credentials.token
    }
    try:
        response = requests.get(parameter_store_url, headers=headers)
        response.raise_for_status()
        response=response.json()
    # 全ての例外を捕捉
    except Exception as e:
        print("Failed of Parameter Store requisition.")
        if(retry_count < 2):
            # リトライ2回未満ならカウント+1してもう一回
            retry_count+=1
            get_ssm_parameter(ssm_parameter_path,retry_count)
        else:
            raise e from None
    else:
        # エラーが起こらなければ値を返却
        return response["Parameter"]["Value"]


def get_request_url():
    """OpeansearchのホストURLを取得する関数

    Returns:
        str: ホストURLもしくはfalse
    """
    host_url= get_ssm_parameter(ssm_parameter_key['auth_key'],0)
    return host_url

def get_host_url():
    """ホストURLを取得する関数

    Returns:
        str: ホストURLもしくはfalse
    """
    host_url= get_ssm_parameter(ssm_host_parameter_key['auth_key'],0)
    return host_url

def put_index(url,aws_auth,document,header,retry_count):
    """requestsを使用してインデックスドキュメントを作成する関数

    Args:
        url (Str): put通信するURL
        aws_auth (AWS4Auth): AWSの認証情報
        document (dict): インデックスの内容
        header (dict): ヘッダ情報
        retry_count (int): リトライ数
    """
    try:
        put_response=requests.put(url, auth=aws_auth,json=document, headers=header)
        #400系、500系のエラー返却時例外発生    
        put_response.raise_for_status()
        put_response=put_response.json()
        if(put_response["result"]!="created" and put_response["result"]!="updated"):
            raise Exception("Create request result is not 'created' or 'updated'.") from None
    except Exception as e:
        print("Failed of  Create or update to index document.")
        if(retry_count <2):
            retry_count +=1
            put_index(url,aws_auth,document,header,retry_count)
        else:
            raise e from None
    
def delete_index(url,aws_auth,header,retry_count):
    """インデックスを削除する関数
    
    Args:
        url (Str): put通信するURL
        aws_auth (AWS4Auth): AWSの認証情報
        header (dict): ヘッダ情報
        retry_count (int): リトライ数

    Raises:
        Exception: aossとの通信で発生するエラー
    """
    try:
        # DELETE でドキュメント削除
        del_response= requests.delete(url,auth=aws_auth, headers=header)
        # 400系、500系のコード返却で例外発生
        del_response.raise_for_status()
        del_response=del_response.json()
        if(del_response["result"]!="deleted"):
        #    返却結果がdeletedではない場合例外発生
            raise Exception("Delete request result is not 'deleted'.") from None
    except Exception as e:
        print("Failed of Delete index.")
        if(retry_count <2):
            # リトライ2回未満ならインクリメントして再送
            retry_count+=1
            delete_index(url,aws_auth,header,retry_count)
        else:
            # 2回目のリトライでエラーが出た場合異常終了
            raise e from None

# インデックス作成処理
def lambda_handler(event, context):
    for record in event['Records']:

        # S3のレコードからバケット名とオブジェクトキーを取得
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # .html が末尾につかない場合、処理を中止
        if not key.endswith(".html"):
            break

        # 一部を除きindex.html はインデックス作成しない
        if key.endswith("index.html"):
            if key.endswith("price/index.html"):
                pass
            elif  key.endswith("information/service/index.html"):
                break
            elif  key.endswith("service/index.html"):
                pass
            else:
                break

        # メンテナンスページはインデックス作成しない
        if key.endswith("maintenance.html"):
            break

        # 検索結果 はインデックス作成しない
        if key.endswith("search_result.html"):
            break

        # エラー画面 はインデックス作成しない
        if "error/" in key:
            break
        
        # index_ はインデックス作成しない
        if "index_" in key:
            break

        # フォーム送信完了画面 はインデックス作成しない
        if "inquiry_form_comp.html" in key:
            break

        # ログアウト はインデックス作成しない
        if "logout.html" in key:
            break

        if record['eventName'] == 'ObjectCreated:Put':
            # 指定されたS3バケットからオブジェクトを取得
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
            except ClientError as e:
                print("Failed of get to s3 object.")
                print(e)
                continue
            else:    
                #分割したデータをjson形式で保存 
                html_data = obj['Body'].read().decode('utf-8')
                soup = bs4.BeautifulSoup(html_data,'html.parser')
                document = create_index(soup,key)

                # ドキュメントIDとしてkeyを利用
                document_id = key

                # "/"を削除
                path_without_slash = document_id.replace("/", "")

                # aossのホストURL取得
                oss_host_url= get_request_url()
                
                # ".html"を削除
                cleaned_document_id = path_without_slash.replace(".html", "")
                # OpenSearchへインデックスデータをポストする
                    # response = requests.get(f'{oss_host_url}/{index}/{datatype}/{cleaned_document_id}', auth=awsauth, headers=headers)
                put_index(f'{oss_host_url}/{index}/{datatype}/{cleaned_document_id}',awsauth,document,headers,0)
                    # if response.status_code == 200:
                    #     # ドキュメントが存在する場合は更新
                    #     r = requests.put(f'{oss_host_url}/{index}/{datatype}/{cleaned_document_id}', json=document,auth=awsauth, headers=headers)
                    #     print('update')
                    # elif response.status_code == 404:
                    #     # ドキュメントが存在しない場合は新規追加
                    #     r = requests.put(f'{oss_host_url}/{index}/{datatype}/{cleaned_document_id}', json=document,auth=awsauth, headers=headers)
                    #     print('create') 
                    
        elif record['eventName'] == "ObjectRemoved:DeleteMarkerCreated":
            # Deleted Object from MT. 
            document_id = key
            oss_host_url= get_request_url()
            
            if oss_host_url == "false":
                break
            else:
                # "/"を削除
                path_without_slash = document_id.replace("/", "")
                # ".html"を削除
                cleaned_document_id = path_without_slash.replace(".html", "")
                # 指定したドキュメントIDのインデックス削除
                delete_index(f'{oss_host_url}/{index}/{datatype}/{cleaned_document_id}',awsauth, headers,0)
        else:
            # イベント名が違うときの処理
            print('Event name is ')
            print(record['eventName'])
            continue   
    else:
        # 反復処理完了後
        return 'OK'

