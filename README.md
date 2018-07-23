`pipenv install`
`pipenv run python3 main.py &room`
--stealth will cause heimdall to not set its own nick. Other flags exist, can be found at the bottom of main.py
Running more than one instance (i.e. for more than one room) from the same directory is not currently supported. (This should work if heimdall is run via yggdrasil but at the moment it... doesn't. Some bug somewhere in forseti, probably?)
Aliasing is not entirely reliable.
It crashes a lot.
The code is a mess.
Karelia is now maintained by kaliumxyz, not me. So use `pipenv uninstall karelia && pipenv install git+https://github.com/kaliumxyz/karelia` to get it.
Oh, and good luck.
