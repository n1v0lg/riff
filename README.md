# riff
Extending VIFF (MPC framework) with relational operator support.

To run example.py on localhost with three players:

1) Generate Viff config files as described in the Viff documentation:

```
python generate-config-files.py -n 3 -t 1 localhost:9001 localhost:9002 localhost:9003
```

2) In three separate terminals, run:

```
python example.py PATH-TO-VIFF/app/player-1.ini --no-ssl
```

```
python example.py PATH-TO-VIFF/app/player-2.ini --no-ssl
```

```
python example.py PATH-TO-VIFF/app/player-3.ini --no-ssl
```
