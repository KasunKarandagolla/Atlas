import os,runpy,sys
for key in list(os.environ):
 if any(word in key.upper() for word in ('KEY','TOKEN','SECRET','PASSWORD','CREDENTIAL')): os.environ.pop(key)
os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD']='1'
def deny(event,args):
 if event in {'socket.connect','socket.getaddrinfo','socket.gethostbyname','socket.sendto'}: raise RuntimeError('OFFLINE_NETWORK_DENIED')
sys.addaudithook(deny)
if __name__ == '__main__':
 runpy.run_module('pytest',run_name='__main__')
