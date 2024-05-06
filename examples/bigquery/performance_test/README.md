install `locust`

```console
$ python3 -m venv .venv

$ .venv/bin/pip3 install locust

$ .venv/bin/locust -V
```

start master

```console
$ .venv/bin/locust --web-host 127.0.0.1 --master -f dummy.py
```

start worker

```console.
$ go build -o a.out main.go

$ ./a.out -url http://127.0.0.1:8080/sync_test/
```


