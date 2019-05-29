# datastore test

DatastoreのPythonAPIは一発で何件データを引っ張ってこれるかのテスト用コード

## 挙動

1. 1000件ほどDatastoreにランダムなデータを突っ込む
2. 読み出して読めた件数を標準出力に吐き出す

## 動かし方

1. [ここ]<http://compling.hss.ntu.edu.sg/wnja/index.ja.html>から日本語データベースをDLして解凍する。
2. Datastoreへの読み書きが可能な(ユーザ権限を持った)クレデンシャルファイルを`credential.json`として配置
3. `pipenv install`
4. `pipenv run python ./main.py` で起動