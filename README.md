# oblivisync

## Requriments

 1. `fuse` 
 2. python3 : version 3.5.1 or greater 
 3. pycrypt
 
## Installation

To install, first install fuse for your Operating System. On Ubuntu,
```
sudo apt-get install fuse
```

The initialize the `fusepy` submodule

```
git submodule init
git submodule update
```

The installation also depends on PyCrypto. On Ubuntu,

```
sudo apt-get install python-crypto
```

## Execution

Here are the options for ObliviSync:

```
./ObliviSync.py [OPTIONS] <backend> <mountpoint> 

<backend>     : where backend files are stored
<mountpoint>  : where the fuse client mounts

OPTIONS
    -r	      : Read-Only mount
    -h        : print this help screen
    -v        : verbose output
    -d file   : set verbose output to file (dflt: stderr) (use - for stdout)
```

To run with DropBox, choose a backend directory in your DropBox folder.


## Video Demo

(coming soon)

