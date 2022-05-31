import struct
import threading
import socket
import asyncio
import re
from os import mkdir, path
from OpenSSL import crypto
from asyncio import StreamReader, StreamWriter
from pickle import dumps, loads


encode = lambda x: bytes(x, encoding='utf-8')
decode = lambda x: bytes.decode(x)


def log_error(err, ignore_file_name=[]):
    '''
    More beautiful output error logger
    '''
    err = err.strip('\n')
    all_mes = re.findall(r'File.*\n\s+.*', err)

    err_type = err.split('\n')[-1]
    if ':' in err_type:
        err_class, err_message = err_type.split(':', 1)
    else:
        err_class = err_type
        err_message = ''

    output = '========Error Occured========\n'
    before_file = ''

    for i in all_mes:
        state, program = i.split('\n')
        err_file, err_line, err_pos = state.split(', ')

        for i in ignore_file_name:
            if err_file.find(i)!=-1:
                break
        else:
            if before_file:
                output += '-----------------------------\n'
            if err_file!=before_file:
                output += f'Error File   : {err_file[5:]}\n'
                before_file = err_file

            output += f'Error Line   : {err_line[5:]}\n'
            output += f'Error Pos    : {err_pos[3:]}\n'
            output += f'Error program: {program.strip()}\n'

    output += '=============================\n'
    output += f'Error Class  : {err_class}\n'
    output += f'Error Message: {err_message}\n'
    output += '============================='
    print(output)


def recv_msg(sock: socket.socket):
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    return loads(recvall(sock, msglen))


def recvall(sock: socket.socket, n: int):
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data


def send_with_len(sock: socket.socket, data):
    data = dumps(data)
    data = struct.pack('>I', len(data)) + data
    sock.sendall(data)


def write_with_len(writer: StreamWriter, data):
    data = dumps(data)
    data = struct.pack('>I', len(data)) + data
    writer.write(data)


async def write_with_len_async(writer: StreamWriter, data):
    data = dumps(data)
    data = struct.pack('>I', len(data)) + data
    writer.write(data)
    await writer.drain()


async def read_msg(reader: StreamReader):
    raw_msglen = await readall(reader, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    data = await readall(reader, msglen)
    return loads(data)


async def readall(reader: StreamReader, n: int):
    data = b''
    while len(data) < n:
        packet = await reader.read(n - len(data))
        if not packet:
            return None
        data += packet
    return data


class AoiThread(threading.Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self,  *args, **kwargs):
        super(AoiThread, self).__init__(*args, **kwargs)
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


class ThDict(dict):
    '''
    a thread safe dict object
    '''

    def __init__(self, *args, **kwargs):
        super(ThDict, self).__init__(*args, **kwargs)
        self.lock = threading.RLock()

    def __getitem__(self, k):
        with self.lock:
            return super(ThDict, self).__getitem__(k)

    def __getitem__(self, k):
        with self.lock:
            return super(ThDict, self).__getitem__(k)

    def __delitem__(self, k):
        with self.lock:
            return super(ThDict, self).__delitem__(k)

    def __setitem__(self, k, v):
        with self.lock:
            return super(ThDict, self).__setitem__(k, v)

    def __contains__(self, k):
        with self.lock:
            return super(ThDict, self).__contains__(k)

    def __iter__(self):
        with self.lock:
            yield from super(ThDict, self).__iter__()

    def keys(self):
        with self.lock:
            yield from super(ThDict, self).keys()

    def values(self):
        with self.lock:
            yield from super(ThDict, self).values()

    def items(self):
        with self.lock:
            yield from super(ThDict, self).items()


def cert_gen(
  emailAddress="emailAddress",
  commonName="commonName",
  countryName="NT",
  localityName="localityName",
  stateOrProvinceName="stateOrProvinceName",
  organizationName="organizationName",
  organizationUnitName="organizationUnitName",
  serialNumber=0,
  validityEndInSeconds=10*365*24*60*60,
  PATH = '.',
  KEY_FILE = "private.key",
  CERT_FILE = "selfsigned.crt"):
    '''
    generate ssl key and cert
    '''

    #can look at generated file using openssl:
    #openssl x509 -inform pem -in selfsigned.crt -noout -text
    # create a key pair
    k = crypto.PKey()
    k.generate_key(crypto.TYPE_RSA, 4096)

    # create a self-signed cert
    cert = crypto.X509()
    cert.get_subject().C = countryName
    cert.get_subject().ST = stateOrProvinceName
    cert.get_subject().L = localityName
    cert.get_subject().O = organizationName
    cert.get_subject().OU = organizationUnitName
    cert.get_subject().CN = commonName
    cert.get_subject().emailAddress = emailAddress
    cert.set_serial_number(serialNumber)
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(validityEndInSeconds)
    cert.set_issuer(cert.get_subject())
    cert.set_pubkey(k)
    cert.sign(k, 'sha512')

    #save cert and key
    if not path.isdir(PATH):
        mkdir(PATH)

    with open(f'{PATH}/{CERT_FILE}', "wt") as f:
        f.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert).decode("utf-8"))
    with open(f'{PATH}/{KEY_FILE}', "wt") as f:
        f.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, k).decode("utf-8"))
