import socket 
import pickle
import sys
import hashlib
import ast
import random
from threading import Thread
import chord_populate
import chord_query

M = 7  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
NODES = 2**M
BUF_SZ = 4096  # socket recv arg
BACKLOG = 100  # socket listen arg
TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n

# Map of the ports and ids of nodes
MAP_OF_ID_PORTS = dict()
#startting port number to generate more ports
STARTING_PORT = 52341

"""
ModRange class
"""
class ModRange(object):
    def __init__(self, start, stop, divisor):
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor

        # we want to use ranges to make things speedy, but if it wraps around the 0 node, we have to use two
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        elif self.stop == 0:
            self.intervals = (range(self.start, self.divisor),)
        else:
            self.intervals = (range(self.start, self.divisor), range(0, self.stop))

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.stop, self.divisor)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        for interval in self.intervals:
            if id in interval:
                return True
        return False

    def __len__(self):
        total = 0
        for interval in self.intervals:
            total += len(interval)
        return total

    def __iter__(self):
        return ModRangeIter(self, 0, -1)

"""
ModRangeIter class
"""
class ModRangeIter(object):
    """ Iterator class for ModRange """
    def __init__(self, mr, i, j):
        self.mr, self.i, self.j = mr, i, j

    def __iter__(self):
        return ModRangeIter(self.mr, self.i, self.j)

    def __next__(self):
        if self.j == len(self.mr.intervals[self.i]) - 1:
            if self.i == len(self.mr.intervals) - 1:
                raise StopIteration()
            else:
                self.i += 1
                self.j = 0
        else:
            self.j += 1
        return self.mr.intervals[self.i][self.j]

"""
FingerEntry class

"""
class FingerEntry(object):                
    def __init__(self, n, k, node=None):
        #print("n " + str(n), "K " + str(k))
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (n + 2**(k-1)) % NODES
        self.next_start = (n + 2**k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.next_start, self.node)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        return id in self.interval

"""
chord_node class
"""
class ChordNode(object):
    """function to print finger table"""
    def print_finger_table(self):
        print("Table is: ")
        for i in range(1, M + 1):
            print("start: " + str(self.finger[i].start), "interval " + str(self.finger[i].interval.intervals), "node = " + str(self.finger[i].node))
        print("predecessor is " + str(self.predecessor))


    """function to initialize finger table"""
    def init_finger_table(self, n, n_prime):
        self.finger[1].node = self.send_pickled_message(n_prime, "find_successor", [self.finger[1].start])
        print("called find successor by:"+str(self.node), "to: " + str(n_prime))
        self.predecessor = self.send_pickled_message(self.successor, "find_predecessor", [self.finger[1].node])
        print("called find predecessor by:"+str(self.node), "to: " + str(self.successor))
        self.send_pickled_message(self.successor, "set_predecessor", [n])
        print("called set_predecessor by:"+str(self.node), "to: " + str(self.successor))
        for i in range (1, M):
            if self.finger[i + 1].start in ModRange(n, self.finger[i].node, NODES):
                self.finger[i + 1].node = self.finger[i].node  
            else:
                if n_prime == self.finger[i + 1].start:
                    self.finger[i + 1].node = n_prime
                else:
                    self.finger[i + 1].node = self.send_pickled_message(n_prime, "find_successor",[self.finger[i + 1].start])

    """function to find a successor"""
    def find_successor(self, id):
        if id == self.node:
            return self.successor

        n_prime = self.find_predecessor(id)

        if n_prime == self.node:
            return self.successor
        else:
            return self.send_pickled_message(n_prime, "find_successor", [n_prime])
            print("called find_successor by:"+str(self.node), "to: " + str(n_prime))
            
    """function to find predecessor"""
    def find_predecessor(self, id):
        n_prime = self.node
        current_node_successor = self.finger[1].node

        if n_prime == current_node_successor:
            return n_prime

        while id not in ModRange(n_prime + 1, current_node_successor + 1, NODES):
            #id <= n_prime or id > current_node_successor:
            if n_prime == self.node:
                n_prime = self.closest_preeceding_finger(id)
            else:
                n_prime = self.send_pickled_message( n_prime, "closest_preeceding_finger", [id])

            if n_prime == self.node:
                break

            print("CALLING FROM HERE " + str(n_prime) + " " + str(self.node))
            current_node_successor = self.send_pickled_message( n_prime, "find_successor", [n_prime])

            if n_prime == current_node_successor:
                break

        return n_prime

    """function to find closest preecedinf finger"""
    def closest_preeceding_finger(self, id):
        for i in range(M, 0, -1):
            if self.finger[i].node in ModRange(self.node + 1, id, NODES):
                #self.node < self.finger[i].node and self.finger[i].node < id:
                return self.finger[i].node

        return self.node

    """function to update other nodes, to update their finger tables"""
    def update_others(self):
        """ Update all other node that should have this node in their finger tables """
        for i in range(1, M+1):  # find last node p whose i-th finger might be this node
            print("I is " + str(i))
            print("Node is " + str(self.node))
            val = (1 + self.node - 2**(i-1) + NODES) % NODES
            print("Value is " + str(val))
            # FIXME: bug in paper, have to add the 1 +
            p = self.find_predecessor(val)
            print("Found p " + str(p))
            if p != self.node:
                self.send_pickled_message(p, 'update_finger_table', [self.node, i])
                print("called update_finger_table by:"+str(self.node), "to: " + str(p) + "with i " + str(i))
            
    """function to update finger table of a node"""
    def update_finger_table(self, s, i):
        """ if s is i-th finger of n, update this node's finger table with s """
        # FIXME: don't want e.g. [1, 1) which is the whole circle
        if (self.finger[i].start != self.finger[i].node
            # FIXME: bug in paper, [.start
            and s in ModRange(self.finger[i].start, self.finger[i].node, NODES)):
            print('update_finger_table({},{}): {}[{}] = {} since {} in [{},{})'.format(s, i, self.node, i, s, s, self.finger[i].start, self.finger[i].node))
            self.finger[i].node = s
            print('#', self)
            p = self.predecessor  # get first node preceding myself
            self.send_pickled_message(p, 'update_finger_table', [s, i])
            print("called update_finger_table by:"+ str(self.node), "to: " + str(p))
            print("DONE UPDATING")
            self.print_finger_table()
            return str(self)
        else:
            print("DID NOTHING")
            self.print_finger_table()
            return 'did nothing {}'.format(self)

    """function to join into the network"""
    def join(self, n, n_prime):
        if n_prime != None:
            self.init_finger_table(n, n_prime)
            self.update_others()
        else:
            for i in range(1, M + 1):
                self.finger[i].node = n
            self.predecessor = n

        print("------------------------------finger table-------------------------------------")
        self.print_finger_table()

    """function to pickle message and send it to remote node"""
    def send_pickled_message(self, remote_id, function_name, argument_list):
        global MAP_OF_ID_PORTS
        #print("Remote id: " + str(remote_id))
        remote_port = MAP_OF_ID_PORTS[remote_id]
        remote_address = ("127.0.0.1", remote_port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sending_socket:
            try:
                print("Sending to " + function_name +  str(remote_address))
                sending_socket.connect(remote_address)
                sending_socket.send(pickle.dumps((function_name, argument_list)))
                recieved_data = pickle.loads(sending_socket.recv(4096))
                return recieved_data
            except Exception as er:
                print(er)

    """function to create a listening thread and handle incoming messages"""
    def handle_incoming_messages(self, my_address):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listening_socket:
            try:
                listening_socket.bind(my_address)
                listening_socket.listen(10)
            except Exception as er:
                print(er)

            while True:
                connection, address = listening_socket.accept()
                t = Thread(target=self.handle_message_thread, args= (connection,))
                t.start()

    """function to call methods based on the unmarshalled message"""
    def handle_message_thread(self, connection):
        data = pickle.loads(connection.recv(4096))
        function_name, arguments = data
        print("connection recieved : " + str(function_name), " " + str(arguments))

        if function_name == "find_predecessor":
            id = arguments[0]
            result = self.find_predecessor(id)
            connection.sendall(pickle.dumps(result))
        elif function_name == "find_successor":
            id = arguments[0]
            result = self.find_successor(id)
            connection.sendall(pickle.dumps(result))
        elif function_name == "closest_preeceding_finger":
            id = arguments[0]
            result = self.closest_preeceding_finger(id)
            connection.sendall(pickle.dumps(result))
        elif function_name == "update_finger_table":
            s = arguments[0]
            i = arguments[1]
            self.update_finger_table(s,i)
            connection.sendall(pickle.dumps(()))
        elif function_name == "update_others":
            self.update_others()
        elif function_name == "set_predecessor":
            node = arguments[0]
            self.set_predecessor(node)
            connection.sendall(pickle.dumps(()))
        elif function_name == "find_correct_node_for_key":
            id = arguments[1]
            result = self.find_successor(id)
            connection.sendall(pickle.dumps(result))
        elif function_name == "add_key_to_node":
            new_key_value = arguments[0]
            key = new_key_value[0]
            value = new_key_value[1]
            self.keys[key] = value
            print("Node id = " , self.node)
            print("keys are: ")
            for i in self.keys:
                print("Key is :" + str(i), "value is: " + str(self.keys[i]))
        elif function_name == "find_node_for_query":
            key_query = arguments[0]
            hash_val = hashlib.sha1(key_query.encode())
            encoded_hash_val = hash_val.hexdigest()
            find_int_equivalent_of_hex_num = int(encoded_hash_val,16)
            node_id = find_int_equivalent_of_hex_num % (2**M)
            result = self.find_successor(node_id)
            connection.sendall(pickle.dumps(result))
        elif function_name == "query":
            hashed_key = arguments[0]
            if hashed_key in self.keys.keys():
                result = self.keys[hashed_key]
            else:
                result = "Key not found"
            connection.sendall(pickle.dumps(result))

    """function to set all variables for a node"""
    def __init__(self, n):
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
            #print(find_int_equivalent_of_hex_num)

            node_id = find_int_equivalent_of_hex_num % (2**M)
            if node_id not in MAP_OF_ID_PORTS:
                MAP_OF_ID_PORTS[node_id] = STARTING_PORT

            STARTING_PORT = STARTING_PORT + 1
        
        random_number = random.randint(0, (2**M) - 1)
        #random_number = 2
                
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM)as s:
            try:
                s.setblocking(1) 
                s.bind(("127.0.0.1", MAP_OF_ID_PORTS[random_number]))
                s.listen(1)
            except Exception as er:
                print(er)

            my_address = s.getsockname()
        
        string_port = str(my_address[1])
        new_address = my_address[0] + string_port

        value_to_hash = new_address

        # encoding the address
        encoded_hash = hashlib.sha1(new_address.encode())
        encoded_hash_output = encoded_hash.hexdigest()

        find_int_equivalent_of_hex_num = int(encoded_hash_output,16)

        node_id = find_int_equivalent_of_hex_num % (2**M)

        print("The id of the node is: " + str(node_id)," with address " + str(my_address))

        self.node = node_id
        self.finger = [None] + [FingerEntry(node_id, k) for k in range(1, M+1)]  # indexing starts at 1
        self.predecessor = None
        self.keys = {}

        if port == 0:
            print("Started a new network")
            n_prime = None
        else:
            print("Node added to the nextwork")
            for key, value in MAP_OF_ID_PORTS.items():
                if port == value:
                    id_for_node = key

            n_prime = id_for_node
        print("------------------------------before joing ft-------------------------------------")
        self.print_finger_table()

        Thread(target=self.handle_incoming_messages, args= (my_address,)).start()
        self.join(self.node, n_prime)
        
    @property
    def successor(self):
        return self.finger[1].node

    @successor.setter
    def successor(self, id):
        self.finger[1].node = id

    def set_predecessor(self, id):
        self.predecessor = id
        print("SET PREDECESSOR")
        self.print_finger_table()


"""
Approach used for lookup_node : Base port and offset, where base port is STARTING_PORT

how to start up each of your Chord nodes from the command line 
(what arguments are required for the first node and what arguments are required for subsequent nodes)(2 parameters)-> 
to start chord_node: input is port number of the existing node or 0 to start a new netwok

how to invoke chord_populate from the command line->command(3 parameters): chord_populate <port number of the existing node> <filename(csv file)>

how to invoke chord_query from the command line(3 parameters)->command: chord_query <port number of existing node> <query to be searched(concat of 1st and 4th colum values)>
"""

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("need more arguments")
        exit(1)

    elif len(sys.argv) == 2:
        port = int(sys.argv[1])

    print("The port to be searched: ", port)

    Chord = ChordNode(port)