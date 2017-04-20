import MySQLdb
import datetime

password="password"
db = MySQLdb.connect(host="10.1.1.1", user="user", passwd=password, db="resource", port=3306)
cursor = db.cursor()

epoch = datetime.datetime(1970, 1, 1)
gateway_map = {'CUCC':'10.2.1.1', 'CMCC':'10.2.1.2'}
exist_routes = set(['10.1.1.2'])


def route_scanner():
    routes = {}
    sql = '''select pub_ip,detail_ip from servers where voserver=1 and unix_timestamp(down_time)>%d''' % get_timestamp()
    print sql
    cursor.execute(sql)

    for r in cursor.fetchall():
        ip = r[0]
        route = r[1]
        if 'CUCC' in route or 'CMCC' in route:
            network = get_network(ip)
            gateway = get_gateway(route)
            if network not in exist_routes and network not in routes:
                routes[network] = gateway
    return routes


def get_timestamp(): 
    now = datetime.datetime.utcnow()
    return (now - epoch).total_seconds()


def get_network(ip):
    fields = ip.split('.')
    return '{}.{}.{}.0'.format(fields[0], fields[1], fields[2])


def get_gateway(route):
    fields = route.split(':')
    return gateway_map[fields[0]]


def get_result(routes):
    for key, value in routes.iteritems():
        print 'sudo route add -net %s netmask 255.255.255.0 gw %s' % (key, value)


if __name__ == "__main__":
    routes = route_scanner()
    get_result(routes)
