import logging
import logging.config
import logging.handlers
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

"""
lets log
"""
#os.chdir("/mnt/apps/repo/dovetail-joint/")
#value = os.getenv(, None)
logging.handlers = logging.handlers
# logging.config.fileConfig('logging.conf')
logging.basicConfig(level=logging.DEBUG,
            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
            datefmt='%a, %d %b %Y %H:%M:%S',
            filename='reco_server.log',
            filemode='w')
mylogr = logging.getLogger("TRotatingFileHandler")
eventslogr = logging.getLogger("events")
lotamelogr = logging.getLogger("lotame")
userlogr = logging.getLogger("user")




# Production Cluster
clusterIPs = ["10.2.3.21", "10.2.3.182", "10.2.4.152"]

# Testing
# clusterIPs = ["192.168.99.100"]

keyspace = "uma"
user_data = "user_data_v2"
user_reco = "user_reco"
article_data = "article_data_v2"
article_reco = "article_reco"
