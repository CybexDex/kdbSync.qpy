

from bitshares import BitShares
import config

bitshares = BitShares(config.BTS_NODE)
def reset():
    return BitShares(config.BTS_NODE)

def get_lib():
    try:
        ifo = bitshares.info()
        lib = ifo['last_irreversible_block_num']
    except:
        bitshares = reset()
        ifo = bitshares.info()
        lib = ifo['last_irreversible_block_num']
    return lib

if __name__ == '__main__':
    print(get_lib())



