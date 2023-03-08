import logging
import azure.functions as func
import requests
import json
from datetime import datetime, timedelta, timezone
import pytz
from azure.storage.filedatalake import DataLakeFileClient
from dotenv import load_dotenv
import os

# .envファイルのパスを指定する
load_dotenv('.env')


def main(mytimer: func.TimerRequest) -> None:

    # region 設定値
    jst = pytz.timezone('Asia/Tokyo')
    utc_timestamp = datetime.utcnow().replace(
        tzinfo=timezone.utc).isoformat()
    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    # 開始日時（前日の0時0分0秒）
    start_datetime = datetime.now(jst).replace(
        hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    start_timestamp = int(start_datetime.timestamp())

    # 終了日時（前日の23時59分59秒）
    end_datetime = datetime.now(jst).replace(
        hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
    end_timestamp = int(end_datetime.timestamp())
    # endregion

    # region ユーザーデータ取得
    # Slack API でユーザーデータを取得する
    token = os.environ['SLACK_API_TOKEN']
    users_url = 'https://slack.com/api/users.list'
    headers = {'Authorization': 'Bearer ' + token}
    users_res = requests.get(users_url, headers=headers)
    users_res.raise_for_status()
    users_data = users_res.json()
    users = users_data['members']
    # endregion

    # region チャンネルデータと発言データの取得
    # Slack API でチャンネルデータを取得する
    conv_list_url = 'https://slack.com/api/conversations.list'
    data = {'limit': 1000}
    conv_list_res = requests.get(conv_list_url, headers=headers, params=data)
    conv_list_res.raise_for_status()
    conv_list_data = conv_list_res.json()
    channels = conv_list_data['channels']

    # Slack API で発言データを取得する
    channel_ids = [channel['id'] for channel in channels]
    messages = []
    replies = []
    for channel_id in channel_ids:
        conv_url = 'https://slack.com/api/conversations.history'
        data = {
            'channel': channel_id,
            'limit': 1000,
            # "oldest": start_timestamp,
            "latest": end_timestamp,
            "inclusive": True
        }
        conv_res = requests.post(conv_url, headers=headers, data=data)
        conv_res.raise_for_status()
        conv_data = conv_res.json()

        # スレッド自体の投稿が一日以内だった場合、jsonにメッセージを追加
        get_messages = conv_data['messages']
        extract_messages = [
            message for message in get_messages if start_timestamp <= int(float(message['ts']))]
        for extract_message in extract_messages:
            extract_message['channel'] = channel_id
        messages += extract_messages

        # スレッドのラストリプライが一日以内だった場合、当該リプライを追加
        for message in get_messages:
            try:
                if start_timestamp <= int(float(message['latest_reply'])):
                    rep_url = 'https://slack.com/api/conversations.replies'
                    rep_data = {
                        'channel': channel_id,
                        'ts': message['ts'],
                        'limit': 1000,
                        "oldest": start_timestamp,
                        "latest": end_timestamp,
                        "inclusive": True
                    }
                    rep_res = requests.post(
                        rep_url, headers=headers, data=rep_data)
                    replies_data = rep_res.json()['messages'][1:]
                    for replies_single_data in replies_data:
                        replies_single_data['channel'] = channel_id
                    replies += replies_data
            except:
                pass
    # endregion

    # region DataLakeへの保存
    # Data Lake Storage Gen1 に JSON ファイルをアップロードする
    account_name = os.environ['AZURE_ACCOUNT_NAME']
    account_key = os.environ['AZURE_ACCOUNT_KEY']
    container_name = os.environ['AZURE_DATALAKE_CONTAINER_NAME']
    directory_name = 'raw/slack_data/' + \
        start_datetime.strftime('%Y') + '/' + \
        start_datetime.strftime('%m') + '/'
    file_name = start_datetime.strftime('%d') + '.json'
    file_system_client = DataLakeFileClient(
        account_url=f"https://{account_name}.dfs.core.windows.net", credential=account_key)
    file_system_client.create_directory(
        container_name, directory_name, metadata=None, permissions=None, umask=None)
    file_path = directory_name + file_name
    file_client = file_system_client.get_file_client(container_name, file_path)
    data = {'users': users, 'channels': channels,
            'messages': messages, 'replies': replies}
    data_json = json.dumps(data)
    file_client.upload_data(data_json, overwrite=True)
    # endregion
