method to appear
================
    run couchbase-server and test on different host in LAN. and unplug-plug the cable many times. write a view to see is there any document's type is base64. if it appears the problem is appear.

compile command
===============
    g++ -std=c++11 *.cpp -lcouchbase -levent -lcrypto -o test

1.build required
    json            directory that contain jsoncpp's header
    jsoncpp.cpp     jsoncpp's implement
    main.cpp        my test code

2.other files
    key_863.base64  base64 encoded of document mutated
    key_863.json    document mutated. contains some unprintable character, it's better to view with hexdump
    key_863.dump    hexdump of key_863.json
