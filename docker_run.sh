#!/bin/bash

docker run -ti --privileged --cap-add SYS_ADMIN --device /dev/fuse oblivisync/oblivisync  /bin/bash 
