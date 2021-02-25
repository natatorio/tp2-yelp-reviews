import requests
import os
import time
import subprocess


def revive(ip):
    print('Process ' + ip + ' has died. Starting it up again...')
    result = subprocess.run(['docker', 'start', ip], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print('Process ' + ip + ' is up again. Result={}. Output={}. Error={}'.format(result.returncode, result.stdout, result.stderr))

def main():
    ips = []
    for processKey in ['ROUTER', 'STARS5', 'COMMENT', 'BUSSINESS', 'USERS', 'HISTOGRAM', 'FUNNY', 'STARS5_MAPPER', 'COMMENT_MAPPER', 'HISTOGRAM_MAPPER', 'FUNNY_MAPPER']:
        nReplicas = int(os.environ['N_' + processKey])
        processIp = os.environ['IP_' + processKey]
        if nReplicas > 1:
            for i in range(nReplicas):
                ip = os.environ['IP_PREFIX'] + '_' + processIp + '_' + str(i + 1)
                ips.append(ip)
        else:
            ips.append(processIp)
    port = os.environ['HEALTHCHECK_PORT']
    time.sleep(10)
    while True:
        time.sleep(3)
        for ip in ips:
            print('http://' + ip + ':' + port + '/health')
            try:
                requests.get('http://' + ip + ':' + port + '/health')
            except:
                revive(ip)

if __name__ == "__main__":
    main()
