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

class chord_populate(object):

    """function to send a key to remote server for populating"""
    def send_key(self, port,key_and_value_to_send):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM)as s:
            try:
                remote_address = (('127.0.0.1', port))
                function_name = "add_key_to_node"
                argument_list = [key_and_value_to_send]
                print("Sending to " + function_name +  str(remote_address))
                s.connect(remote_address)
                s.send(pickle.dumps((function_name, argument_list)))
            except Exception as er:
                print(er)

    """constructor for the class"""
    def __init__(self,port, filename):
        self.given_port = port
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

        for key, value in MAP_OF_ID_PORTS.items():
                if self.given_port == value:
                    id_for_node = key

        node_id = id_for_node
        
        record__players_dictionary = dict()
        list_of_value = list()
        dictionary_of_keys_and_id = dict()
    
        with open(filename, newline = '') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                concatenate_player_id_year = row['Player Id'] + row['Year']
                encoded_hash = hashlib.sha1(concatenate_player_id_year.encode())
                resulting_hashkey = encoded_hash.hexdigest()
                find_int_equivalent_of_hex_num = int(resulting_hashkey,16)

                node_id_that_has_key = find_int_equivalent_of_hex_num % NODES

                dictionary_of_keys_and_id[resulting_hashkey] = node_id_that_has_key

                list_of_value = [row['Player Id'],row['Name'], row['Position'], row['Year'], row['Team'], row['Games Played'], row['Passes Attempted'], row['Passes Completed'], 
                                row['Completion Percentage'], row['Pass Attempts Per Game'], row['Passing Yards'], row['Passing Yards Per Attempt'], 
                                row['Passing Yards Per Game'], row['TD Passes'], row['Percentage of TDs per Attempts'], row['Ints'], row['Int Rate'], row['Longest Pass'], 
                                row['Passes Longer than 20 Yards'], row['Passes Longer than 40 Yards'], row['Sacks'], row['Sacked Yards Lost'], row['Passer Rating']]

                record__players_dictionary[resulting_hashkey] = list_of_value

        print("the map is")
        print(MAP_OF_ID_PORTS)

        print("port given as input = " + str(self.given_port), "has id: " + str(node_id))

        remote_address = ("127.0.0.1", self.given_port)
        function_name = "find_correct_node_for_key"

        for i in dictionary_of_keys_and_id:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM)as s:
                try:
                    print("Sending to " + function_name +  str(remote_address))
                    s.connect(remote_address)
                    print(len(dictionary_of_keys_and_id))
                    argument_list = [i, dictionary_of_keys_and_id[i]]
                    s.send(pickle.dumps((function_name, argument_list)))
                    recieved_data = pickle.loads(s.recv(4096))
                    port_to_send_key = MAP_OF_ID_PORTS[recieved_data]
                    self.send_key(port_to_send_key, (i, record__players_dictionary[i]))
                except Exception as er:
                    print(er)

            
if __name__ == '__main__':
    print(len(sys.argv))
    if len(sys.argv) != 3:
        print("need more arguments")
        exit(1)

    elif len(sys.argv) == 3:
        port = int(sys.argv[1])
        filename = sys.argv[2]

    print("The port to be provided: ", port)
    print("Name of the file: ", filename)

    chord_populate = chord_populate(port,filename)