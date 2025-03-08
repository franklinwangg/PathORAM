import socket
import pickle
import random
import time
import struct
import json

class Client:
    def __init__(self, host='127.0.0.1', port=65433, num_blocks=10, tree_height=3, bucket_capacity=4):
        self.host = host
        self.port = port
        self.stash = []
        
        self.position_map = dict()
        # fetch the actual random leaf mappings from server

        self.client_socket = None
        
        self.connect_to_server()
        
        self.tree_height = int(self.send_request("get_tree_height"))
        self.bucket_capacity = int(self.send_request("get_bucket_capacity"))
            
    def connect_to_server(self):
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
        self.client_socket.connect((self.host, self.port))
        
    def send_request(self, *args):
        # request = " ".join(map(str, args)) + "\n"
        request = " ".join(map(str, args))

        self.client_socket.send(request.encode('utf-8'))

        if request.startswith("get_tree_height"):
            response = self.client_socket.recv(4)
            if len(response) == 4:
                tree_height = struct.unpack('!I', response)[0]
                return tree_height
            else:
                print("Error: Received unexpected data format for get_tree_height.")
                return None
        if request.startswith("get_bucket_capacity"):
            response = self.client_socket.recv(4)
            if len(response) == 4:
                bucket_capacity = struct.unpack('!I', response)[0]
                return bucket_capacity
            else:
                print("Error: Received unexpected data format for get_tree_height.")
                return None

        elif request.startswith("get_bucket"):
            response = self.client_socket.recv(4096).decode('utf-8')  # Receive the bucket data
            try:
                bucket_data = json.loads(response)  # Deserialize JSON response
                return bucket_data
            except json.JSONDecodeError:
                print("Error: Could not decode bucket data")
                return None
        elif request.startswith("initialize_position_map"):
            # Send request to the server
            self.client_socket.send(request.encode('utf-8'))

            # Receive the response (position map) from the server
            response = self.client_socket.recv(4096).decode('utf-8')

            try:
                # Deserialize the received JSON data
                returned_position_map = json.loads(response)

                # Store or return the position map
                return returned_position_map

            except json.JSONDecodeError:
                print("Error: Could not decode position map data")
                return None

        # After this, only send the get_tree_height request once the position map has been successfully handled
        elif request.startswith("write_bucket"):
            pass
        elif request.startswith("remove_bucket"):
            pass

        else:
            response = self.client_socket.recv(1024).decode('utf-8')  # Receive up to 1024 bytes for text
            return response
    def get_random_leaf_node(self):
        leaf_start = 2 ** (self.tree_height + 1) - 2 ** self.tree_height - 1
        leaf_end = 2 ** (self.tree_height + 1) - 1
        
        random_leaf_node = random.randint(leaf_start, leaf_end - 1)
        
        return random_leaf_node
        
    def access(self, op, a, data=None): # need datablock id?
        
        x = -1
        random_leaf_node = self.get_random_leaf_node()
        
        if(a in self.position_map):
            x = self.position_map[a]

        else:
            x = random_leaf_node

        # 2) position[a] <= x* <= uniformrandom(0... 2^L - 1)
        self.position_map[a] = random_leaf_node

# 3) for L belongs to {0, 1, ... L}, do
# 	stash <= stash union readBucket(P(x, L))
        for l in range(0, self.tree_height + 1, 1):
            bucket_at_level_l = self.P(x, l)

            # basically remove every actual element from the bucket here
            for i in range(len(bucket_at_level_l['bucket'])): 
                if(bucket_at_level_l['bucket'][i]['block_id'] != -1):

                    self.stash.append(bucket_at_level_l['bucket'][i]) # returns the bucket at level l to leafnode x
                    # 1) first time it gets sent as single item to "data", 2) second time it turns into list?

                    self.send_request("remove_bucket ", bucket_at_level_l['bucket_id'], " ", bucket_at_level_l['bucket'][i]['block_id'])
                    # remove it from bucket


# 6) data <= read block a from S
                # if op = write then S <= (S - {(a, x, data)}) U {(a, x*, data*)}
        read_res = None

        if(op == "write"):
            for i in range(len(self.stash) - 1, -1, -1):  # Iterate backwards
                if self.stash[i]['block_id'] == a:
                    self.stash.pop(i)  # Safe to remove

        # self.stash.append((a, random_leaf_node, data))
            self.stash.append({
                "block_id": a, 
                "capacity": 1,  # Assuming capacity is always 1
                "leaf_node": random_leaf_node, 
                "data": data
            })

        
        elif(op == "read"):

            for i in range(len(self.stash)):
                if(self.stash[i]['block_id'] == a):
                    read_res = self.stash[i]
                    break

# 10) for L belongs to {L, L-1, ..., 0}
# 	stash' <= {(a', x', data') belongs to S : P(x, L) = P(x', L)}
# 	stash' <= select min(|S'|, Z) blocks from S'
# 	stash <= stash - stash'
# 	writeBucket(P(x, L), S')

        for l in range(self.tree_height, -1, -1):
            subtract_stash = []
            current_bucket = self.P(x, l)

            for i in range(len(self.stash)):
                temp = self.P(self.stash[i]["leaf_node"], l)

                if(temp == current_bucket): # comparing int to bucket
                    subtract_stash.append(self.stash[i])

            min_blocks = min(len(self.stash), self.bucket_capacity - current_bucket['actual_buckets_count'])

            # remove everything from subtract stash from stash
            first_min_blocks_of_subtract_stash = self.stash[0:min_blocks]
            
            self.stash = [block for block in self.stash if block not in first_min_blocks_of_subtract_stash]
            self.send_request("write_bucket", x, subtract_stash[0:min_blocks])

            
# 11) return data
        if(op == "read"):
            return read_res['data']
        return None

    def P(self, leaf_node, level):  # returns the bucket at this level on the path to this leaf_node
        
        x = self._get_node_at_level(leaf_node,level,self.tree_height)
        
        bucket_at_x = self.send_request(f"get_bucket {x}")
        return bucket_at_x
    
    def _get_node_at_level(self,leaf_index, target_level, tree_height): # returns index(int)
        current_index = leaf_index
        current_level = tree_height 
        while current_level > target_level:
            if current_index == 0:
                break
            current_index = (current_index - 1) // 2 
            current_level -= 1 

        return current_index 
    
if __name__ == "__main__":
    client = Client()
    # Example usage:
    
    print("Writing block 1 with data '1'")
    client.access("write", 1, "1")
    time.sleep(1)
    print("Reading block 1 : ", client.access("read", 1))
    time.sleep(1)

    print("Writing block 2 with data '2'")
    time.sleep(1)

    client.access("write", 2, "2")
    print("Reading block 2 : ", client.access("read", 2))
    
    print("Writing block 3 with data '3'")
    client.access("write", 3, "3")
    print("Reading block 3 : ", client.access("read", 3))
    
    # print("Writing block 4 with data '4'")
    # client.access("write", 4, "4")
    # print("Reading block 4", client.access("read", 4))
    
    # print("Writing block 5 with data '5'")
    # client.access("write", 5, "5")
    # print("Reading block 5", client.access("read", 5))