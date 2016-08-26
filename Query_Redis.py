import redis;
                                                                    # Create redis object connecting it to 'localhost'
                                                                    # Global declaration
r_serv=redis.Redis('localhost');

# Function to query
def query():
    path=raw_input("Enter path of query file: ");                   # Prompt for file path

                                                                    # Object for storing file data
    lists=[];                                                       # To store 'SELECT' attributes
    from_lis=[];                                                    # To store 'FROM' values
    values=[];                                                      # To store 'WHERE' clause
    P_op='';                                                        # Primary Operator
    flag=0;                                                         # Flag variable
    
                                                                    # Read from the file
    data=open(path,'r');
    temp=data.readline();
    temp=temp.rstrip();
    lists=temp.split(',');                                          # 'SELECT' attributes added
    temp=data.readline();
    from_lis=temp.split(',');                                       # 'FROM' attributes added
    temp=data.readline();
    
        
# Looking into WHERE clause
    
    if temp.find('AND')>-1:                                         # Looking for 'AND'  (Intersection)
        P_op='AND';
    if temp.find('OR')>-1:                                          # Looking for 'OR'   (Union)
        P_op='OR';

    if temp.find('AND')==-1 and temp.find('OR')==-1:              # No 'AND' / 'OR' operator found
        P_op='';
        save=handy(temp);                                           # Handy function to sort out things
        for i in save:
            print(r_serv.hmget(i,lists));                           # Final Output

       
        
# Handling Case with AND/OR operator   
        
    if P_op!='':
        values=temp.split(P_op);                                    # String splitted based on primary operator
        temp=values[0];                                             # Handling Left part of 'WHERE' clause
        Lsave=set(handy(temp));                                     # Handy function to sort out things
       
        temp=values[1].replace(" ","");                             # Replace any whitespaces
        Rsave=set(handy(temp));                                     # Handy function to sort out things
          
      
        if P_op=='AND':
            save=Lsave.intersection(Rsave);                         # Intersection of Sets in the case of AND operation

        if P_op=='OR':
            save=Lsave.union(Rsave);                                # Union of Sets in the case of OR operation
    
        for i in save:
            print(r_serv.hmget(i,lists));                           # Final Output
      
#----------------------------------------------------------------------------------------------------------------------------------------------------------#


def handy(temp):
    flag=0;
    save=[];
    if temp.find('=')!=-1:
        M_op='=';
    if temp.find('>')!=-1:
        M_op='>';
    if temp.find('<')!=-1:
        M_op='<';
    values=temp.split(M_op);
    Mrel=values[0];
    Mval=values[1];
        
    if Mrel.find('.')>-1:
        mtemp=Mrel.split('.');
        mtable=mtemp[0];
        mattrib=mtemp[1];
        
            
    if Mval.find('.')>-1:
        mtemp=Mval.split('.');
        mvaltable=mtemp[0];
        mvalattrib=mtemp[1];
        flag=1;

    if flag==0:
        k=r_serv.smembers(mtable);
        arg=[Mrel];
        for i in k:
            if(M_op=='='):
                if r_serv.hmget(i,arg)[0]==Mval:
                    save.append(i);                         # Selected Primary Key's based on Where clause
            if(M_op=='<'):
                if r_serv.hmget(i,arg)[0]<Mval:
                    save.append(i);                         # Selected Primary Key's based on Where clause
            if(M_op=='>'):
                if r_serv.hmget(i,arg)[0]>Mval:
                    save.append(i);                         # Selected Primary Key's based on Where clause
            a=r_serv.hmget(i,arg);
        return(save);                                       # Returning List
            
       

                                                            # Flag case
    if flag==1:
        l=r_serv.smembers(mtable);                          # Returns SET
        r=r_serv.smembers(mvaltable);                       # Returns SET
        save=l.intersection(r);
        return(save);                                       # Returning SET
        
#----------------------------------------------------------------------------------------------------------------------------------------------------------#


def create():
    path=raw_input("Enter path of relation's file: ");                  # Prompt for file path
                                                                        # Object for storing file data
    lists=[];                                                           # To store schema data
    values=[];                                                          # To store values data

                                                                        # Read from the file
    data=open(path,'r');
    table_name=data.readline();
    table_name=table_name.rstrip();
                                                                        #Schema reading begins
    while(1):
        temp_dt=table_name;
        temp_dt+='.';
        temp=data.readline();
        temp=temp.rstrip();
        temp_dt+=temp;
        if(temp==";"):
            break;
        else:
            lists.append(temp_dt);
                                                                        # Schema reading ends
                                                                        # Read values
    while(1):
        values=[];
        query={};
        temp_dt=data.readline();                                        # Reading VALUES line/s
        temp_dt=temp_dt.rstrip();
        if(temp_dt==""):
            break;
        else:
            values=temp_dt;
            temp=values.split(';');
            first_arg=temp[0];
            for i in range(0,len(temp)):
                query[lists[i]]=temp[i];
            r_serv.sadd(table_name,first_arg);
            r_serv.hmset(first_arg,query);
            print(r_serv.hmget(first_arg,query));

    print("Data added Successfully");
    
    
#----------------------------------------------------------------------------------------------------------------------------------------------------------#
   
                                                                        # This is the main menu    
while(1):
    print("Enter 1 to add relation schema and values");
    print("Enter 2 to Query");
    print("Enter 3 to Exit");
    opt=int(raw_input("Enter Choice: "));
    if opt==1:
        create();
    if opt==2:
        query();
    if opt==3:
        exit();
    
