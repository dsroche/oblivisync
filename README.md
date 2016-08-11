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
/ObliviSync.py [OPTIONS] <backend> <mountpoint>
<backend>       : where backend files are stored
<mountpoint>	: where the fuse client mounts

OPTIONS
    -h		: print this help screen
    -r 		: Read-Only mount   (dflt: rw mount)
    -k      	: set the drip rate (dflt: 3)
    -t          : set the drip time (dflt: 3)

    -v      	: verbose output
    -d file     : set verbose output to file (dflt: stderr) (use - for stdout)
	
```

To run with DropBox, choose a backend directory in your DropBox folder.



## Docker

To simplify the setup and demonstration, we have provided a Dockerfile
that you can use to setup a ready made image for running and testing
OblviSync. The Dockerfile will create an image with a user `user` with
headless dropbox installed under `/home/user/.dropbbox-dist` and
ObliviSync installed under `/home/user/oblivisync`.

Two scripts are provided to help this setup:

  - `docker_build.sh` : build the image for the container, based on the ubuntu container
  - `docker_run.sh` : run the image in a container, will give priviledge for mounting and start as `user` in `/home/user` 

## Video Demo

A demovideo can be found here where you can see ObliviSync working
with a shared backend folder. 

[![ObliviSync Demo Video](http://img.youtube.com/vi/-MYgtts_sO8/0.jpg)](http://www.youtube.com/watch?v=-MYgtts_sO8)
