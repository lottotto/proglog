# 8章 合意形成によるサービス連携

raftの説明
- raftが与えられたコマンドを適用する有限ステートマシン
- raftがそれらのコマンドを保存するログストア
- raftがクラスタの構成を保存する安定ストア
- raftがデータのコンパクトなスナップショットを保存するスナップショットストア
- raftが他のraftサーバと接続するために使うトランスポート

上記をsetupRaftで設定しなければならない


ディスカバリの統合
Serf駆動のディスカバリレイヤをraftと統合する必要がある。つまりhandlerを実装すること。
より具体的にいうと、raftが更新されるたびに、joinとleaveメソッドが動くのだが、それをraftに対応する必要がある。
