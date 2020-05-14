# smartProxy
TCP/UDP proxy
![logic diagram](https://github.com/chaoyujiang/style-pictures/blob/master/smartProxy.jpg)


Configuration file:
{
    "testTcp":{
        "Name":"testTcp",
        "SourceAddr":"0.0.0.0:8800",
        "DstBindIp":"0.0.0.0",
        "DstAddr":"10.2.13.50:8800",
        "Protocol":"tcp",
        "AclRule":{
                     "IPNets":["10.1.1.38/24"],
                     "Rule": 1 
        }   
    },  
    "testUdp":{
        "Name":"testUdp",
        "SourceAddr":"0.0.0.0:8801",
        "DstBindIp":"0.0.0.0",
        "DstAddr":"10.2.13.50:8801",
        "Protocol":"udp",
        "AclRule":{
                     "IPNets":["10.1.1.38/24"],
                     "Rule": 1   
        }   
    }   
}

           -----------
SourceAddr|           |DstBindIp     
--------->| smartProxy|---------------> DstAddr
   server |           | client
           -----------
AclRule: IPNets["10.1.1.2/24","2.2.2.2/16"]  Rule: 0 permit; 1 deny

For example: "AclRule":{"IPNets":["10.1.1.38/24"],"Rule": 1 } , The client with ip 10.1.1.18 will be denied, because it is in the subnet 10.1.1.0/24.
