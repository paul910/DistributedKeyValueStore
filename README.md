# DistributedKeyValueStore ([Github Repository](https://github.com/paul910/DistributedKeyValueStore))

Die Anwendung enthält Codeausschnitte aus bereits bestehenden Anwendungen aus dem Internet.

## Autoren
- Fabian Albrecht fabian.albrecht01@sap.com
- Paul Ehrhard paul.ehrhard@sap.com
## Installation

- Python 3.6 oder höher
- install pip3
- ``git clone git@github.com:paul910/DistributedKeyValueStore.git``
- ``cd DistributedKeyValueStore``
- ``pip3 install -r requirements.txt``

## Starten

- Netwerk starten mit der ``test.sh``-Datei: ``bash test.sh``, vier Nodes werden gestartet
- Manuellen Client starten: ``python3 client/manual_client.py``

## Funktionsweise
Jede Node kann abgeschaltet werden. Wenn der Leader jedoch deaktiviert wird, wird automatisch ein neuer Leader bestimmt. 
Der Manual Client, verwendet grundlegende Netzwerkprogrammierung, um mit dem Leader im Netzwerk zu interagieren. 
Der Manual Client wendet die Set- und Get-Befehle auf den Leader an. 
Dieser wiederum verteilt die Befehle an die restlichen Nodes, speichert die Sets in der log-Datei und liefert ein Ergebnis zurück.

Key-Value-Pairs können über den ``manual_client.py`` gesetzt(SET) und abgefragt(GET) werden.

