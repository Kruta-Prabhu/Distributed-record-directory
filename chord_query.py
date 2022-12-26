import socket
import pickle
import csv
import hashlib
import chord_node
import sys

M = 7  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
NODES = 2**M

MAP_OF_ID_PORTS = dict()
STARTING_PORT = 52341

class chord_query(object):

    """function to send the key for querying """
    def send_key(self, port,hashed_key):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM)as s:
            try:
                remote_address = (('127.0.0.1', port))
                function_name = "query"
                argument_list = [hashed_key]
                print("Sending to " + function_name +  str(remote_address))
                s.connect(remote_address)
                s.send(pickle.dumps((function_name, argument_list)))
                recieved_data = pickle.loads(s.recv(4096))
                value_found = recieved_data
                if recieved_data == "Key not found":
                    print("-----------Key NOT found-----------")
                else:
                    print("==============Data found=============")
                    print("Player id ", value_found[0])
                    print("Name ", value_found[1])
                    print("Position", value_found[2])
                    print("Year ", value_found[3])
                    print("Team ", value_found[4])
                    print("Games Played ", value_found[5]) 
                    print("Passes Attempted", value_found[6]) 
                    print("Passes Completed ", value_found[7]) 
                    print("Completion Percentage ", value_found[8]) 
                    print("Pass Attempts Per Game ", value_found[9]) 
                    print("Passing Yards ", value_found[10]) 
                    print("Passing Yards Per Attempt", value_found[11]) 
                    print("Passing Yards Per Game ", value_found[12])  
                    print("TD Passes ", value_found[13]) 
                    print("Percentage of TDs per Attempts ", value_found[14]) 
                    print("Ints ", value_found[15]) 
                    print("Int Rate ", value_found[16]) 
                    print("Longest Pass ", value_found[17]) 
                    print("Passes Longer than 20 Yards ", value_found[18]) 
                    print("Passes Longer than 40 Yards ", value_found[19]) 
                    print("Sacks ", value_found[20]) 
                    print("Sacked Yards Lost ", value_found[21]) 
                    print("Passer Rating ", value_found[22])
                     
            except Exception as er:
                    print(er)

    """constructor for the class"""
    def __init__(self, port, query):
        self.port = port
        self.query = query_key

        global MAP_OF_ID_PORTS
        global STARTING_PORT
        while len(MAP_OF_ID_PORTS) < (2**M):
            string_port = str(STARTING_PORT)
            address = '127.0.0.1' + string_port

            value_to_hash = address

            # encoding the address
            hash_val = hashlib.sha1(address.encode())
            encoded_hash_val = hash_val.hexdigest()

            find_int_equivalent_of_hex_num = int(encoded_hash_val,16)

            node_id = find_int_equivalent_of_hex_num % (2**M)
            if node_id not in MAP_OF_ID_PORTS:
                MAP_OF_ID_PORTS[node_id] = STARTING_PORT

            STARTING_PORT = STARTING_PORT + 1

        remote_address = ("127.0.0.1", self.port)

        function_name = "find_node_for_query"
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM)as s:
            try:
                print("Sending to " + function_name +  str(remote_address))
                s.connect(remote_address)
                argument_list = [query]
                s.send(pickle.dumps((function_name, argument_list)))
                recieved_data = pickle.loads(s.recv(4096))
                print("print recieved node id is ",recieved_data)
                hash_val = hashlib.sha1(self.query.encode())
                encoded_hash_val = hash_val.hexdigest()
                hashed_query = encoded_hash_val
                port_to_send_key = MAP_OF_ID_PORTS[recieved_data]
                self.send_key(port_to_send_key, hashed_query)
            except Exception as er:
                print(er)

        
if __name__ == '__main__':
    print(len(sys.argv))
    if len(sys.argv) != 3:
        print("need more arguments")
        exit(1)

    elif len(sys.argv) == 3:
        port = int(sys.argv[1])
        query_key = sys.argv[2]

    print("The port to be provided: ", port)
    print("query key: ", query_key)

    chord_query = chord_query(port,query_key)