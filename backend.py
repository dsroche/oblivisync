import os
from Crypto.Cipher import AES
from Crypto.Hash import HMAC, SHA256
from rwlock import get_rw_locks
from lru import LRUlist

readSize = 1048576

class BackendError(Exception): pass    

@LRUlist(10)
class Backend:
    #Key should be a 16 byte array or 16 length string
    def __init__(self, key, directory):
        self.key = key
        self.directory = directory

        files = os.listdir(directory)

        self.length = 0
        for i in files:
            try:
                fnum = int(i)
            except ValueError:
                # ignore extraneous files
                continue
            if fnum >= self.length:
                self.length = fnum + 1

        self.rlock, self.wlock = get_rw_locks()

    def _is_stale(self, index, timestamp):
        """For the LRU cache; checks the timestamp in the filesystem."""
        name = os.path.join(self.directory, str(index))
        return os.path.getmtime(name) > timestamp

    def __getitem__(self, index):
        with self.rlock:
            if index < 0:
                raise IndexError("index out of bounds for backend")
            
            name = os.path.join(self.directory, str(index))
            if not os.path.exists(name):
                raise IndexError("file doesn't exist on backend")

            if index >= self.length:
                with self.wlock:
                    self.length = index + 1

            contents = bytes()

            with open(name, "rb") as f:
                chunk = f.read(readSize)
                while chunk:
                    contents += chunk
                    chunk = f.read(readSize)

            return self.decrypt(contents)

    def __setitem__(self, index, data):
        with self.wlock:
            if index < 0:
                raise IndexError("index out of bounds for backend")
            elif index >= self.length:
                self.length = index+1

            dest = os.path.join(self.directory, str(index))
            tdest = dest + '.temp'
            with open(tdest, "wb") as f:
                f.write(self.encrypt(data))
            os.replace(tdest, dest)

    #Expects an array of byte arrays to extend the storage with, 
    def extend(self, newdata):
        with self.wlock:
            for data in newdata:
                self.length += 1
                self[self.length-1] = data

    # extend with a single byte array
    def append(self, newdata):
        with self.wlock:
            self.length += 1
            self[self.length-1] = newdata

    def encrypt(self, plaintext):
        iv = os.urandom(16)

        cipher = AES.new(self.key, AES.MODE_CFB, iv)
        ciphertext = iv + cipher.encrypt(plaintext)

        hmac = HMAC.new(self.key, digestmod=SHA256)
        hmac.update(ciphertext)
        mac = hmac.digest()

        return mac + ciphertext

    def decrypt(self, ciphertext):
        mac = ciphertext[:32]
        ciphertext = ciphertext[32:]
        hmac = HMAC.new(self.key, digestmod=SHA256)
        hmac.update(ciphertext)
        testmac = hmac.digest()

        if mac != testmac:
            raise BackendError("MAC verification failed!")

        iv = ciphertext[:16]
        ciphertext = ciphertext[16:]

        cipher = AES.new(self.key, AES.MODE_CFB, iv)
        return cipher.decrypt(ciphertext)

    def __len__(self):
        return self.length
