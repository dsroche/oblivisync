FROM ubuntu

#initialize directories
RUN apt-get update && apt-get upgrade -y &&\
apt-get install -y emacs24-nox python python-crypto python3 python3-crypto tmux fuse wget sudo &&\
groupadd -r user -g 1001 &&\
useradd -u 1001 -r -g 1001 -d /home/user -s /bin/bash -c "Book User" user && \
mkdir /home/user &&\
chown -R user:user /home/user

RUN mkdir /home/user/oblivisync && mkdir /home/user/oblivisync/fusepy
COPY *.py /home/user/oblivisync/
COPY fusepy/* /home/user/oblivisync/fusepy/
RUN cd /home/user/oblivisync && rm -f fuse.py && ln -s fusepy/fuse.py fuse.py && chown -R user:user /home/user/oblivisync

USER user
RUN mkdir /home/user/bin &&\
    wget -O - "https://www.dropbox.com/download?dl=packages/dropbox.py" > /home/user/bin/dropbox.py &&\
    chmod +x /home/user/bin/dropbox.py &&\
    echo "export PATH=$PATH:/home/user/bin" > /home/user/.bash_profile &&\
    cd /home/user &&  wget -O - "https://www.dropbox.com/download?plat=lnx.x86_64" | tar xzf -

user root
ENTRYPOINT ["sudo", "-u", "user", "-i"]
CMD ["cd", "/home/user"]


