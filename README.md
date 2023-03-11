# get_slackchat_for_azure_functions

## このリポジトリはなに？
Azure DataLakeにSlackデータをjson形式で蓄積していくためのコードです。  
このコードをAzure Functionsにデプロイし、環境変数を設定して日次駆動するようにすれば、前日分のSlackの  
- 全ユーザー情報
- 全オープンチャンネルリスト
- 全メッセージ（スレッドトップ）
- 全リプライ  

を単一のjsonファイルとしてAzure DataLakeのコンテナに格納します。

## 環境変数

ローカルで動かしたいとき、'.env.sample'を'.env'にリネームし、中のキーを適時書き換える。
