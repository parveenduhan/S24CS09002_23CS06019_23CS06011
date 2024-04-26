/* Joint project done by PARVEEN(S24CS09002), AKSHAY(23CS06019), PRANAV(23CS06011)
    SHARED MEMORY is accessed on SYSTEM 2(i.e. this system)*/

#include <cstdio>
#include <queue>
#include <vector>
#include <thread>
#include <mutex>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <functional>
#include <fstream>

using namespace std;

const char *S1_IP = "172.31.27.220"; // System 1 IP
const char *S3_IP = "172.31.26.40";  // System 3 IP
const int S1_PORT = 8080;
const int S3_PORT = 8081;

struct SYS
{
    /* This structure is used to send and receive request/reply to and from systems */

    char msg_type; // R(request) for request, G(Go-ahead) for positive reply and W(wait) for negative reply, 'A' means, it's my turn and let me access
    int ts;        // time stamp at which req to access the resource is made
    char *name;    // name of the system;
};

struct comparator
{
    bool operator()(const SYS *a, const SYS *b)
    {
        return a->ts < b->ts;
    }
};
class System
{
private:
    int ts; // time stamp of this system
    mutex m;
    SYS *s1;
    vector<int> *sock_buf; // buffer storing sockets for all other systems, used to send request msg to all

    priority_queue<SYS *, vector<SYS *>, comparator> req_buf;               // priority queue based buffer to store the requests from other systems
    int ack_buf[3];                                                         // this buffer will hold ack recived(including it's own ack for itself by seeing the queue)
                                                                            //  buf[0] == s1, buf[1] == s2, buf[2] == s3;
    unordered_map<string, int> nameMap = {{"s1", 0}, {"s2", 1}, {"s3", 2}}; // system's name encoding
    unordered_map<string, int> sockMap;

public:
    System()
    {
        this->ts = 0;
        this->s1 = new SYS();
        this->s1->name = "s1";
        this->s1->msg_type = 'R'; // by default it's a request msg. Need to be set while sending
        this->sock_buf = new vector<int>(2);
        //  this->sockMap = new unordered_map<string,int>(2);
    }
    int setupSock(int conn_type)
    {
        /* conn_type == 0 means this system is a server, conn_type == 1 means this system is a client
        A system can be a server as well as client at the same time */

        int sock = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in address;
        address.sin_family = AF_INET;
        if (conn_type == 1)
        {
            address.sin_port = htons(S3_PORT);
            address.sin_addr.s_addr = inet_addr(S3_IP);

            if (connect(sock, (struct sockaddr *)&address,
                        sizeof(address)) != 1)
                perror("connection failed");

            return sock;
        }
        address.sin_port = htons(S1_PORT);
        address.sin_addr.s_addr = INADDR_ANY;

        bind(sock, (struct sockaddr *)&address,
             sizeof(address));

        listen(sock, 5);

        int conn_sock = accept(sock, nullptr, nullptr);
        return conn_sock;
    }

    void localEvent()
    {
        this->m.lock();
        this->ts++; // updating timestamp on local event
        this->m.unlock();
    }

    void sendReq()
    {
        this->m.lock();
        this->s1->ts = this->ts++;
        this->s1->msg_type = 'R';
        m.unlock();

        string send_buf;
        serialize(this->s1, &send_buf);
        /* Sending request to all */
        for (auto &sock : this->sock_buf)
        {
            send(sock, &(send_buf), sizeof(send_buf), 0);
        }
    }
    void serialize(SYS *slz_struct, string *send_buf)
    {
        /* this function will convert the content of the SYS structure into a string for the sake of transmission over network
            Which can again be converted to the SYS structure at receiving end using deserialize() function*/

        string ts = to_string(slz_struct->ts);
        string name = slz_struct->name;
        char separator = 'X'; // this byte will be used to separate different fields of SYS like msg_type, ts and nam
        send_buf = slz_struct->msg_type + separator + ts + separator + name;
    }
    void deserialize(SYS *received_req, string recv_buf)
    {
        /* this function will extract the information from the string buffere received from a system to a structure of type SYS*/
        string ts;
        string name;
        char msg_type;
        msg_type = recv_buf[0];

        size_t firstXIndex = recv_buf.find('X');
        size_t secondXIndex = recv_buf.find('X', firstXIndex + 1);

        ts = recv_buf.substr(firstXIndex + 1, secondXIndex - firstXIndex - 1);
        name = recv_buf.substr(secondXIndex + 1);

        received_req->msg_type = msg_type;
        received_req->ts = stoi(ts);
        received_req->name = const_cast<char *>(name.c_str());
    }

    void sendAck(SYS *reqSys, int sock, char ack_msg)
    {
        /* Ack can be sent to a request made by someone, or by this system if it has sucessfully accessed the critical section
        to someone whoese turn is next. ack_msg can be 'G' ->POSITIVE ACK, 'W' -> NEGATIVE ACK/WAIT, 'F'-> RESOURCES FREED*/

        SYS *topSys = this->req_buf.top();

        this->m.lock();
        int ack_ts = this->ts++;
        this->s1->msg_type = ack_msg;
        this->s1->ts = ack_ts;
        this->m.unlock();
        string send_buf;
        this->serialize(this->s1, &send_buf);
        send(sock, &send_buf, sizeof(send_buf), 0);
    }
    void receiveEvents(int sock)
    {
        /* Recieve Event from remote system using socket*/
        while (true)
        {
            SYS *received_msg;
            string recv_buf;
            read(sock, &recv_buf, sizeof(recv_buf));
            this->deserialize(received_msg, recv_buf);
            this->m.lock();
            this->ts = max(this->ts + 1, received_msg->ts); // updating time stamp on receive event
            this->m.unlock();
            if (received_msg->msg_type == 'R') // if it's a request, add it to the queue and send ack accordingly
            {
                this->req_buf.push(received_msg);
                char ack_msg;

                (received_msg->ts < req_buf.top()->ts) ? ack_msg = 'G' : ack_msg = 'W';

                this->sendAck(received_msg, sock, ack_msg);
            }
            else // if it's a reply msg. take note of it
            {
                this->handleAck(received_msg);
            }
        }
    }
    void handleAck(SYS *received_msg)
    {
        string name = received_msg->name;
        int sys_num = this->nameMap[name];
        switch (received_msg->msg_type)
        {
        case 'G':
            this->ack_buf[sys_num] = 1;
            break;
        case 'W':
            this->ack_buf[sys_num] = 0;
            break;
        case 'F':
            this->req_buf.pop(); // F means release/free => remove entry from the queue
            break;
        case 'A':
            this->accessResource(received_msg); // A means it's my turn, this system has the shared memory so let me access it

        default:
            break;
        }
        SYS *topSys = req_buf.top();
        (this->s1->ts < topSys->ts) ? this->ack_buf[0] = 1 : ack_buf[0] = 0; // if this has high priority it can set ack = 1 for itself
        bool access_flag = true;
        for (int i = 0; i < 3; i++)
        {
            if (ack_buf[i] == 0)
                access_flag = false;
        }
        if (access_flag)
        {
            this->req_buf.pop();      // it's your turn, obvisiouly you are on the top, remove yourself and go ahead to access resource.
            accessResource(this->s1); // TRY TO ACESS RESOURCE when you have all the acks positive
        }
    }
    void accessResource(SYS *req_sys)
    {

        ofstream myfile;
        myfile.open("example.txt");
        myfile << "TS:" << req_sys->ts << " written by:" << req_sys->name;
        myfile.close();
        if (req_sys->name == this->s1->name) // this system accessed the resources.
        {
            SYS *topSys = req_buf.top(); // send 'F' ack to someone who was second and now first.
            string name = topSys->name;
            char ack_msg = 'F';
            this->sendAck(topSys, this->sockMap[topSys->name], ack_msg);
        }
        else // some other system acessed the resources, send him the ack 'D' means done and then that system can send 'F' to concerned system
        {
            char ack_msg = 'D';
            this->sendAck(req_sys, this->sockMap[req_sys->name], ack_msg);
        }
    }

    int getTimestamp()
    {
        return this->ts;
    }
    void addSock(int sock, string name)
    {
        this->sock_buf->push_back(sock);
        this->sockMap[name] = sock;
    }
};

int main()
{
    /*conn_type == 1 means this system is a client and conn_type == 0 means this is a server.
    In this case, this system(S2) is client for S3 and server for S1*/

    System localSystem;
    int conn_type = 1; // conn_type = 1 means this system is a client, = 0 means this is a server.

    int S3_sock = localSystem.setupSock(conn_type);
    localSystem.addSock(S3_sock, "s3");

    conn_type = 0;

    int S1_sock = localSystem.setupSock(conn_type);
    localSystem.addSock(S1_sock, "s1");

    thread receiverThread1([&localSystem, S1_sock]()
                           { localSystem.receiveEvents(S1_sock); }); // separate thread for receiving messages from S1
    thread receiverThread2([&localSystem, S3_sock]()
                           { localSystem.receiveEvents(S3_sock); }); // separate thread for receiving messages from S3

    /*
         Perform localEvent, sendEvent and sendReq events here:

         For e.g. localSystem.sendEvent(S3_sock)
                  localSystem.localEvent();

    */

    // Join the receiver thread
    receiverThread1.join();
    receiverThread2.join();
    fprintf(stdout, "Timestamp is: %d\n", localSystem.getTimestamp());

    return 0;
}
