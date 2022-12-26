# Distributed-record-directory
Implemented the application to store the record of the players of the National Football League from a file and allowed users to search the details of the player using the attributes.
The application was implemented using the Distributed hash table(DHT), which included creating, adding the nodes to the network and implemeting the finger table for every node


Approach used for lookup_node : Base port and offset, where base port is STARTING_PORT

how to start up each of your Chord nodes from the command line 
(what arguments are required for the first node and what arguments are required for subsequent nodes)(2 parameters)-> 
to start chord_node: input is port number of the existing node or 0 to start a new netwok

how to invoke chord_populate from the command line->command(3 parameters): chord_populate <port number of the existing node> <filename(csv file)>
  

how to invoke chord_query from the command line(3 parameters)->command: chord_query <port number of existing node> <query to be searched(concat of 1st and 4th colum values)>
