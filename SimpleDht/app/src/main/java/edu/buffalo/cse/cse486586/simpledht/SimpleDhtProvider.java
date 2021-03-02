package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static edu.buffalo.cse.cse486586.simpledht.SimpleDhtContract.SimpleDhtDB.TABLE_NAME;

public class SimpleDhtProvider extends ContentProvider {


    static final String[] REMOTE_PORT = {"11108","11112","11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    static final String TAG = "SimpleDhtGenericMessage";
    static final String providerUri = "content://edu.buffalo.cse.cse486586.simpledht.provider";
    String delimiter = "<GMP>";

    ArrayList<String> activeNodeList = new ArrayList<String>();

    //for debugging
    ArrayList<String> activePortList = new ArrayList<String>();


    String myPortNumber;
    String portStr;
    String myHash;
    String centralNode = "5554";

    String myPred;
    String mySucc;
    String myPredHash;
    String mySuccHash;

    String joinRequest = "JOIN";
    String joinAck = "JOIN_ACK";
    String orderChanged = "ORDER_CHANGE";
    String orderChangedAck = "ORDER_CHANGE_ACK";
    String deleteRequest = "DELETE";
    String deleteAck = "DELETE_ACK";
    String queryRequest = "QUERY";
    String queryAck = "QUERY_ACK";
    String insertRequest = "INSERT";
    String insertAck = "INSERT_ACK";
    Boolean insertOverride = false;
    String sourcePortString = "0";
    Boolean queryOverride = false;



    HashMap<String, Integer> portMap = new HashMap<String, Integer>();
    HashMap<String, String> processMap = new HashMap<String, String>();
    HashMap<String, String> hashTable = new HashMap<String, String>();


    SimpleDhtDatabase dbHelper;
    SQLiteDatabase dbWrite;
    SQLiteDatabase dbRead;

    //******************************************************* ON CREATE **************************************************************************************************

    @Override
    public boolean onCreate() {
        Context context = getContext();

        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPortNumber = String.valueOf((Integer.parseInt(portStr) * 2));

        portMap.put("5554",11108);
        portMap.put("5556",11112);
        portMap.put("5558",11116);
        portMap.put("5560",11120);
        portMap.put("5562",11124);

        processMap.put("5554","0");
        processMap.put("5556","1");
        processMap.put("5558","2");
        processMap.put("5560","3");
        processMap.put("5562","4");

        try {
            myHash = genHash(portStr);
            hashTable.put(genHash("5554"), "5554");
            hashTable.put(genHash("5556"), "5556");
            hashTable.put(genHash("5558"), "5558");
            hashTable.put(genHash("5560"), "5560");
            hashTable.put(genHash("5562"), "5562");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        Log.d(TAG,portStr);

        //Database Related
        dbHelper = new SimpleDhtDatabase(getContext());
        dbWrite = dbHelper.getWritableDatabase();
        dbRead = dbHelper.getReadableDatabase();

        //Creating the server socket
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            //Log.e(TAG, "Can't create a ServerSocket");
        }

        if(portStr.equals(centralNode)){
        //adding locally
            String[] msg = new String[0];
            msg = addNodesIntoActiveList(portStr).split(":");

            myPred = msg[0];
            mySucc = msg[1];
            try {
                myPredHash = genHash(myPred);
                mySuccHash = genHash(mySucc);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }else{
            //send a message to AVD0 to add me into the active list
            String joinRequestMessage = "NULL"+delimiter+joinRequest+delimiter+portStr+delimiter+centralNode;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,joinRequestMessage);
        }
        return true;
    }

    //******************************************************* QUERY **************************************************************************************************

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {


        if( (myPred.equals("0") && mySucc.equals(("0"))) || (myPred.equals(portStr) && mySucc.equals(portStr))){
            Cursor cursor;
            if(selection.equals("@") || selection.equals("*")){
                cursor = dbRead.rawQuery("select * from "+ TABLE_NAME,null);
                return cursor;
            }else{
                selectionArgs = new String[] {selection};
                selection = SimpleDhtContract.SimpleDhtDB.Key + " = ?";
                cursor = dbRead.query(TABLE_NAME, null, selection, selectionArgs, null,null,null);
                return cursor;
            }
        }



        else if(selection.equals("@")){
            Cursor cursor;
            cursor = dbRead.rawQuery("select * from "+ TABLE_NAME,null);
            Log.i("QUERY","CURSOR TO STRING looks like "+cursor.toString());
            return cursor;
        }



        else if(selection.equals("*")){
            Cursor cursor;
            MatrixCursor returnCursor = new MatrixCursor(new String[] {"key", "value"});
            cursor = dbRead.rawQuery("select * from "+ TABLE_NAME,null);
            cursor.moveToFirst();
            String msgToSend;
            int count = 0;

            if(!sourcePortString.equals("0")){
                 msgToSend = "NULL"+delimiter+queryRequest+delimiter+sourcePortString+delimiter+mySucc;
                 sourcePortString = "0";
            }else{
                 msgToSend = "NULL"+delimiter+queryRequest+delimiter+portStr+delimiter+mySucc;
            }

            String response = null;
            try {
                response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend).get();
            } catch (InterruptedException e) {
                Log.e("QUERY","ERROR WHILE WAITING FOR RESPONSE FROM CLIENT FOR SUCCESSOR QUERY");
                e.printStackTrace();
            } catch (ExecutionException e) {
                Log.e("QUERY","ERROR WHILE WAITING FOR RESPONSE FROM CLIENT FOR SUCCESSOR QUERY");
                e.printStackTrace();
            }

            String [] valuesArray = response.split(":");
            while (!cursor.isAfterLast()) {
                returnCursor.newRow().add("key",cursor.getString(0)).add("value",cursor.getString(1));
                cursor.moveToNext();
                count = count+1;
            }

            for ( String dataPoint : valuesArray ) {
                if(dataPoint.equals("")){
                    continue;
                }
                String key = dataPoint.split(",")[0];
                String value = dataPoint.split(",")[1];
                returnCursor.newRow().add("key", key).add("value", value);
                count = count+1;
            }

            Log.i("QUERY","* Query returning "+count+" values to its server/grader");
            return returnCursor;
        }
        else{

            // SELECTION parameter contains a key

            Log.i("QUERY","Selection parameter is a key "+selection);

            String msgToSend;
            String givenHash = null;
            try {
                givenHash = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            Log.i("QUERY","CONDITIONS "+ myPredHash.compareTo(givenHash) +" "+ mySucc.compareTo(givenHash) +" "+ myHash.compareTo(givenHash) +" "+myHash.compareTo(mySuccHash)+" "+myHash.compareTo(myPredHash));


            if( (givenHash.compareTo(myPredHash)>=0 && givenHash.compareTo(myHash) < 0 ) || (givenHash.compareTo(myPredHash)>=0 && myPredHash.compareTo(myHash) > 0) || (queryOverride)){

                Log.i("QUERY","FOUND : Selection parameter is locally available because either normal case or forwarded from last node to first node or this is first node "+selection);
                Cursor cursor;
                selectionArgs = new String[] {selection};
                selection = SimpleDhtContract.SimpleDhtDB.Key + " = ?";
                cursor = dbRead.query(TABLE_NAME,   // The table to query
                        null,             // The array of columns to return (pass null to get all)
                        selection,              // The columns for the WHERE clause
                        selectionArgs,          // The values for the WHERE clause
                        null,                   // don't group the rows
                        null,                   // don't filter by row groups
                        null               // The sort order
                );
                queryOverride = false;
                return cursor;



            }

            else if(givenHash.compareTo(myHash) < 0 && givenHash.compareTo(myPredHash) <0){

                //first node
                Cursor cursor;
                if(myPredHash.compareTo(myHash) > 0){
                    Log.i("QUERY","SINGLE QUERY FOUND : Selection parameter is locally available because hash lesser than first node hash "+selection);
                    selectionArgs = new String[] {selection};
                    selection = SimpleDhtContract.SimpleDhtDB.Key + " = ?";
                    cursor = dbRead.query(TABLE_NAME,   // The table to query
                            null,             // The array of columns to return (pass null to get all)
                            selection,              // The columns for the WHERE clause
                            selectionArgs,          // The values for the WHERE clause
                            null,                   // don't group the rows
                            null,                   // don't filter by row groups
                            null               // The sort order
                    );

                    return cursor;
                }

                else{
                    //probably before predecessor. call client
                    Log.i("QUERY","SINGLE QUERY FORWARDED : Selection parameter is forwarded because hash lesser than predecessor node hash hence its before predecessor "+selection);

                    MatrixCursor returnCursor = new MatrixCursor(new String[] {"key", "value"});

                    msgToSend = selection+delimiter+queryRequest+delimiter+portStr+delimiter+mySucc+delimiter+"FALSE";
                    String response = "";

                    try {
                          response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }


                    String key = response.split(",")[0];
                    String value = response.split(",")[1];
                    returnCursor.newRow().add("key", key).add("value", value);
                    return returnCursor;

                }
            }
            else if(givenHash.compareTo(myHash)>0 && myHash.compareTo(mySuccHash)>0 ){
                //last node therefore it must be present in the succ. Set flag to true

                Log.i("QUERY","SINGLE QUERY FORWARDED : Selection parameter is forwarded because this is last note and hash is still bigger, it should be in the successor. FLag is set "+selection);

                msgToSend = selection+delimiter+queryRequest+delimiter+portStr+delimiter+mySucc+delimiter+"TRUE";
                String response = "";

                try {
                    response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

                MatrixCursor returnCursor = new MatrixCursor(new String[] {"key", "value"});
                String key = response.split(",")[0];
                String value = response.split(",")[1];
                returnCursor.newRow().add("key", key).add("value", value);

                return returnCursor;
            }
            else{
                Log.i("QUERY","SINGLE QUERY FORWARDED : Normal Case "+selection);

                msgToSend = selection+delimiter+queryRequest+delimiter+portStr+delimiter+mySucc+delimiter+"FALSE";
                String response = "";

                try {
                    response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

                MatrixCursor returnCursor = new MatrixCursor(new String[] {"key", "value"});
                String key = response.split(",")[0];
                String value = response.split(",")[1];
                returnCursor.newRow().add("key", key).add("value", value);

                return returnCursor;
            }

        }
    }

    //******************************************************* DELETE **************************************************************************************************
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        if(myPred.equals("0") && mySucc.equals(("0"))){
            if(selection.equals("@") || selection.equals("*")){
                dbRead.delete(TABLE_NAME,null,null);
                return 0;
            }
        }else if(selection.equals("@")){
            dbRead.delete(TABLE_NAME, null, null);
        }
        else if(selection.equals("*")){

            String msgToSend;

            if(!sourcePortString.equals("0")){
                msgToSend = "NULL"+delimiter+ deleteRequest+ delimiter+portStr+delimiter+mySucc+delimiter+sourcePortString;
                sourcePortString = "0";
            }
            else{
                msgToSend = "NULL"+delimiter+ deleteRequest +delimiter+portStr+delimiter+mySucc+delimiter+portStr;
            }
            dbRead.delete(TABLE_NAME,null,null);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend);
        }else {
            Log.i("DELETE","THe key to be deleted is "+selection);

            String msgToSend;
            String msgHash = "";
            try {
               msgHash = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            if(activeNodeList.size()==1){
                Log.i("DELETE","Only node in list hence the key to be deleted locally is "+selection);
                String[] whereArgs = {selection};
                dbWrite.delete(TABLE_NAME, SimpleDhtContract.SimpleDhtDB.Key + "=?", whereArgs);
            }
            else if(myHash.compareTo(myPredHash)<0 && (msgHash.compareTo(myHash)<0 || msgHash.compareTo(myPredHash)>=0)){
                Log.i("DELETE","FIRST NODE CASE key to be deleted locally is "+selection);
                String[] whereArgs = {selection};
                dbWrite.delete(TABLE_NAME, SimpleDhtContract.SimpleDhtDB.Key + "=?", whereArgs);
            }else if(msgHash.compareTo(myHash)<0 && msgHash.compareTo(myPredHash)>=0){
                Log.i("DELETE","NORMAL CASE key to be deleted locally is "+selection);
                String[] whereArgs = {selection};
                dbWrite.delete(TABLE_NAME, SimpleDhtContract.SimpleDhtDB.Key + "=?", whereArgs);
            }else{
                dbRead.delete(TABLE_NAME, null, null);
                msgToSend = selection+delimiter+ deleteRequest +delimiter+portStr+delimiter+mySucc;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend);
            }
        }
        return 0;
    }


    //******************************************************* INSERT **************************************************************************************************
    @Override
    public Uri insert(Uri uri, ContentValues values) {



        //MSG FORMAT : values.get("key")+":"+values.get("value")+delimiter+insetRequest+delimiter+portStr+delimiter+mySucc+delimiter+"FALSE"
        String msgHash = null;
        String msgToSend;

        try {
             msgHash = genHash(values.getAsString("key"));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (!myPred.equals("0") && !mySucc.equals("0")){
            Log.i("INSERT","CONDITIONS "+ myPredHash.compareTo(msgHash) +" "+ mySucc.compareTo(msgHash) +" "+ myHash.compareTo(msgHash) +" "+myHash.compareTo(mySuccHash)+" "+myHash.compareTo(myPredHash));

        }

        if (myPred.equals("0") && mySucc.equals("0")){
            dbWrite.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
            Log.i("INSERT","Inserting "+values.toString()+" ONLY NODE : local insert without central node");
            return uri;
        }
        else if (insertOverride){
            //Local insert because msg received from last node in the list. Current node is first node
            dbWrite.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
            Log.i("INSERT","Inserting "+values.toString()+" because msg received from last node in the list. Current node is first node in the system");
            insertOverride = false;
            return uri;
        }

        else if(activeNodeList.size() == 1 && myHash.compareTo(myPredHash) == 0 && myHash.compareTo(mySuccHash) == 0 ){
            //Local insert single node for AVD1
            dbWrite.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
            Log.i("INSERT","Inserting "+values.toString()+" locally because only node in system");
            return uri;
        }

        else if( myHash.compareTo(msgHash) > 0 && myPredHash.compareTo(msgHash) <=0){
            //Local insert normal case
            dbWrite.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
            Log.i("INSERT","Inserting "+values.toString()+" locally because normal case msgHash is smaller than node hash and greater than predecessor hash");
            return uri;
        }

         else if( myHash.compareTo(msgHash) > 0 && myPredHash.compareTo(msgHash) > 0 ){

             if (myHash.compareTo(myPredHash) < 0){
                 //means that current node is first node and the message hash is lower than first node
                 //hence insert locally
                 dbWrite.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                 Log.i("INSERT","Inserting "+values.toString()+" locally because msgHash is lower than first node hash");
                 return uri;
             }

            //message to be inserted somewhere before my predecessor so just forward it to next port
            msgToSend = values.get("key")+":"+values.get("value")+delimiter+ insertRequest +delimiter+portStr+delimiter+mySucc+delimiter+"FALSE";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend);
            Log.i("INSERT","Forwarding "+values.toString()+" to be inserted somewhere before predecessor hash because msgHash is less than predecessor hash");
        }

        else if(myHash.compareTo(msgHash) <=0 && myHash.compareTo(mySuccHash) > 0){

            //message forwarded to next node because current node is last node and msgHash is greater than last node hash. Msg marked as must deliver
            msgToSend = values.get("key")+":"+values.get("value")+delimiter+ insertRequest +delimiter+portStr+delimiter+mySucc+delimiter+"TRUE";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend);
            Log.i("INSERT","Forwarding "+values.toString()+" because this is last node and msgHash is greater than last node");
        }

        else if(myHash.compareTo(msgHash) <=0){
            //normal case. Msg Hash is greater than myHash hence forward it to next port.
            msgToSend = values.get("key")+":"+values.get("value")+delimiter+ insertRequest +delimiter+portStr+delimiter+mySucc+delimiter+"FALSE";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend);
            Log.i("INSERT","Forwarding "+values.toString()+" because msgHash is greater than node hash : NORMAL CASE");
        }
        else{
            Log.i("INSERT","UNHANDLED");
            Log.i("INSERT","CONDITIONS"+myPredHash.compareTo(msgHash)+" "+mySucc.compareTo(msgHash)+" "+myHash.compareTo(msgHash)+" "+myHash.compareTo(mySuccHash)+" "+myHash.compareTo(myPredHash));
        }

        return uri;

    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }


    // ******************************************************** HASH FUNCTION ***********************************************************************************************
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }



    //******************************************************* SERVER TASK **************************************************************************************************
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            Socket serSocket=null;
            Socket portBroadcastSocket;

            try{

                while(true){
                    Log.i("SERVER PORT","My Port : "+portStr);
                    Log.i("SERVER PORT","My Pred : "+myPred);
                    Log.i("SERVER PORT","My Succ : "+mySucc);
                    serSocket = serverSocket.accept();
                    ObjectInputStream inMessage = new ObjectInputStream(serSocket.getInputStream());
                    String messageReceived = (String) inMessage.readObject();
                    String[] messageReceivedArray = messageReceived.split(delimiter);
                    Log.i("SERVER","MESSAGE RECEIVED : "+messageReceived);

                    //CATCHES JOIN REQUESTS FROM CLIENTS
                    if(messageReceivedArray[1].equals(joinRequest)){

                        String msg = addNodesIntoActiveList(messageReceivedArray[2]);
                        Log.i("SERVER","JOIN : "+ messageReceivedArray[2]+" added to ACTIVE NODE LIST : "+activePortList.toString());

                        String messageToSend = msg+delimiter+joinAck+delimiter+portStr+delimiter+messageReceivedArray[2];
                        ObjectOutputStream outMessage = new ObjectOutputStream(serSocket.getOutputStream());
                        outMessage.writeObject(messageToSend);
                        Log.i("SERVER",  "JOIN_ACK : Sending pred and succ");
                        outMessage.flush();
                        serSocket.close();

                        //Right after the change in order of nodes, broadcast their new predecessors and successors
                        String localChange = generatePredSucc(portStr);
                        myPred = localChange.split(":")[0];
                        mySucc = localChange.split(":")[1];
                        myPredHash = genHash(myPred);
                        mySuccHash = genHash(mySucc);


                        //ORDER CHANGE MESSAGES
                        for(String remoteHash : activeNodeList){

                            String remotePort = hashTable.get(remoteHash);
                            if( remotePort.equals(messageReceivedArray[2]) || remotePort.equals(centralNode)){
                                continue;
                            }

                            portBroadcastSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(remotePort)*2));
                            msg = generatePredSucc(remotePort);
                            messageToSend = msg+delimiter+orderChanged+delimiter+portStr+delimiter+remotePort;
                            ObjectOutputStream broadCastMessage = new ObjectOutputStream(portBroadcastSocket.getOutputStream());
                            broadCastMessage.writeObject(messageToSend);
                            Log.i("SERVER", "Broadcasting predecessor and successor of port"+remotePort+" : "+msg);
                            outMessage.flush();
                            portBroadcastSocket.close();
                        }

                    }


                    //CATCHES ORDER CHANGE MESSAGES FROM OTHER SEVERS
                    else if(messageReceivedArray[1].equals(orderChanged)){
                        myPred = messageReceivedArray[0].split(":")[0];
                        mySucc = messageReceivedArray[0].split(":")[1];
                        myPredHash = genHash(myPred);
                        mySuccHash = genHash(mySucc);
                        Log.i("SERVER","ORDER_CHANGE : changed predecessor and successor : "+messageReceivedArray[0]);
                        serSocket.close();

                        //sending order change ack
//                        String messageToSend = "NULL"+delimiter+orderChangedAck+delimiter+portStr+delimiter+messageReceivedArray[2];
//                        ObjectOutputStream outMessage = new ObjectOutputStream(serSocket.getOutputStream());
//                        outMessage.writeObject(messageToSend);
//                        Log.i("SERVER",  "ORDER_CHANGE_ACK : Acknowledged by port "+messageReceivedArray[3]);
//                        outMessage.flush();

                    }

                    //CATCHES INSERT REQUESTS FROM CLIENTS
                    else if(messageReceivedArray[1].equals(insertRequest)){
                        ContentValues values = new ContentValues();
                        String key = messageReceivedArray[0].split(":")[0];
                        String value = messageReceivedArray[0].split(":")[1];

                        Log.i("SERVER",messageReceivedArray[0]+" Received as insert request. Taking action");

                        values.put("key", key);
                        values.put("value",value);


                        if(messageReceivedArray[4].equals("TRUE")){
                            //flag that shows if the received message should be compulsorily stored in the current node
                            insertOverride = true;
                        }
                        //Sending insert request ack
                        String messageToSend = "NULL"+delimiter+insertAck+delimiter+portStr+delimiter+messageReceivedArray[2];
                        ObjectOutputStream outMessage = new ObjectOutputStream(serSocket.getOutputStream());
                        outMessage.writeObject(messageToSend);
                        Log.i("SERVER",  "INSERT_ACK : Insert request acknowledged by port "+messageReceivedArray[3]);
                        outMessage.flush();
                        serSocket.close();

                        Log.i("SEVER",values.toString() + " Sent to local insert, will be inserted or forwarded from there");
                        insert(Uri.parse(providerUri), values);
                    }

                    //CATCHES QUERY REQUEST
                    else if(messageReceivedArray[1].equals(queryRequest)){
                        Cursor cursor;
                        String msg = "";
                        String key;
                        String value;

                        if(!messageReceivedArray[0].equals("NULL")){

                            if(messageReceivedArray[4].equals("TRUE")){
                                queryOverride = true;
                                Log.i("SERVER","Selection parameter will be returned by the current node becasue flag is set "+messageReceivedArray[0]);

                            }

                            cursor = query(Uri.parse(providerUri),null, messageReceivedArray[0],null,null );
                            Log.i("QUERY","CURSOR : got  "+cursor.getCount() + "values");
                            cursor.moveToFirst();
                            key = cursor.getString(0);
                            value = cursor.getString(1);
                            msg = key+","+value;
                            Log.i("SERVER","retrieved "+msg);

                        }

                        else if(messageReceivedArray[2].equals(mySucc)){
                            cursor = dbRead.rawQuery("select * from "+ TABLE_NAME,null);
                            if(cursor!=null){
                                for (cursor.moveToFirst();!cursor.isAfterLast();cursor.moveToNext()) {
                                    key = cursor.getString(0);
                                    value = cursor.getString(1);
                                    msg = msg+key+","+value+":";
                                }

                            }
                            cursor.close();

                        }
                        else{
                            sourcePortString = messageReceivedArray[2];
                            cursor = query(Uri.parse(providerUri),null,"*", null,null);
                            if(cursor!=null) {
                                for (cursor.moveToFirst();!cursor.isAfterLast();cursor.moveToNext()) {
                                    key = cursor.getString(0);
                                    value = cursor.getString(1);
                                    msg = msg+key+","+value+":";
                                }

                            }
                            cursor.close();
                        }

                        ObjectOutputStream outMessage = new ObjectOutputStream(serSocket.getOutputStream());
                        outMessage.writeObject(msg);
                        Log.i("SERVER",  "QUERY_ACK : Sending retrived result back to +"+messageReceivedArray[2]);
                        outMessage.flush();
                        serSocket.close();
                    }
                    else if(messageReceivedArray[2].equals(deleteRequest)){

                        if(messageReceivedArray[0].equals("NULL")){
                            if(messageReceivedArray[4].equals(mySucc)){
                                dbRead.delete(TABLE_NAME,null,null);
                            }else{
                                sourcePortString = messageReceivedArray[4];
                                delete(Uri.parse(providerUri),"*",null);
                            }

                        }else{
                            delete(Uri.parse(providerUri),messageReceivedArray[0],null);
                        }

                        serSocket.close();
                    }


                }

            } catch (OptionalDataException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
                Log.i("SERVER","Server Side I/O exception");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }


            return null;
        }
    }






    //******************************************************* CLIENT TASK **************************************************************************************************
    private class ClientTask extends AsyncTask<String, Void, String> {

        Socket socket;
        @Override
        protected String doInBackground(String...  msgs) {
            try {
                String[] messageArray = msgs[0].split(delimiter);


                //SENDING JOIN REQUEST
                if (messageArray[1].equals(joinRequest)) {

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portMap.get(centralNode));
                    ObjectOutputStream outMessage = new ObjectOutputStream(socket.getOutputStream());
                    outMessage.writeObject(msgs[0]);
                    Log.i("CLIENT","JOIN : Sending Join Request from port "+portStr);
                    outMessage.flush();


                    //Receiving Pred and Succ as ack from central node
                    ObjectInputStream inMessage = new ObjectInputStream(socket.getInputStream());
                    String messageReceived = (String) inMessage.readObject();
                    String[] receivedMessageArray = messageReceived.split(delimiter);
                    Log.i("CLIENT","JOIN_ACK : Received");
                    socket.close();

                    myPred = receivedMessageArray[0].split(":")[0];
                    mySucc = receivedMessageArray[0].split(":")[1];
                    myPredHash = genHash(myPred);
                    mySuccHash = genHash(mySucc);

                }

                else if(messageArray[1].equals(insertRequest)){
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(messageArray[3]) * 2);
                    ObjectOutputStream outMessage = new ObjectOutputStream(socket.getOutputStream());
                    outMessage.writeObject(msgs[0]);
                    Log.i("CLIENT","INSERT FORWARDING : Sending message "+messageArray[0]+ "to successor "+messageArray[3]);
                    outMessage.flush();


                    //Receiving Insert Request ack
                    ObjectInputStream inMessage = new ObjectInputStream(socket.getInputStream());
                    String messageReceived = (String) inMessage.readObject();
                    Log.i("CLIENT","INSERT_FORWARD_ACK : Received");
                    socket.close();
                }

                else if(messageArray[1].equals(queryRequest)){

                    if(messageArray[0].equals("NULL")){
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(messageArray[3]) * 2);
                        ObjectOutputStream outMessage = new ObjectOutputStream(socket.getOutputStream());
                        outMessage.writeObject(msgs[0]);
                        Log.i("CLIENT","Asking for single selection query from successor "+messageArray[3] + " for selection "+messageArray[0]);
                        outMessage.flush();


                        //Receiving Query Response
                        Log.i("CLIENT","Waiting for response from server for response to selection "+messageArray[0]);
                        ObjectInputStream inMessage = new ObjectInputStream(socket.getInputStream());
                        String messageReceived = (String) inMessage.readObject();
                        Log.i("CLIENT","Got reponse from server "+messageReceived);
                        socket.close();
                        return messageReceived;
                    }

                    else{
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(messageArray[3]) * 2);
                        ObjectOutputStream outMessage = new ObjectOutputStream(socket.getOutputStream());
                        outMessage.writeObject(msgs[0]);
                        Log.i("CLIENT","Asking for single selection query from successor "+messageArray[3] + " for selection "+messageArray[0]);

                        Log.i("CLIENT","Asking for query from successor "+messageArray[3]);
                        outMessage.flush();


                        //Receiving Query Response
                        ObjectInputStream inMessage = new ObjectInputStream(socket.getInputStream());
                        String messageReceived = (String) inMessage.readObject();
                        Log.i("CLIENT","QUERY_REQUEST_ACK : Received");
                        socket.close();
                        return messageReceived;
                    }

                }else if(messageArray[2].equals(deleteRequest)){
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(messageArray[3]) * 2);
                    ObjectOutputStream outMessage = new ObjectOutputStream(socket.getOutputStream());
                    outMessage.writeObject(msgs[0]);
                    Log.i("CLIENT","DELETE REQUEST to successor  "+messageArray[3]);
                    outMessage.flush();
                    socket.close();
                }

            }catch (UnknownHostException e) {
                e.printStackTrace();
                Log.i("CLIENT","Unknown Host Exception during JOIN message");
            } catch (IOException e) {
                e.printStackTrace();
                Log.i("CLIENT","I/O exception during JOIN message");
                myPred = "0";
                mySucc = "0";
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            return null;
        }
    }


    // ******************************************************** HELPER FUNCTIONS ***********************************************************************************************

    private String addNodesIntoActiveList(String port){

        String portHash = null;
        try{
            portHash = genHash(port);
        }catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        activeNodeList.add(portHash);
        Collections.sort(activeNodeList);
        int size = activeNodeList.size();
        int insertIndex = activeNodeList.indexOf(portHash);
        String pred;
        String succ;

        if(size==1){
            pred = port;
            succ = port;
        }else if(insertIndex == 0){
            succ = hashTable.get(activeNodeList.get(1));
            pred = hashTable.get(activeNodeList.get(size-1));
        }else if(insertIndex == size-1){
            pred = hashTable.get(activeNodeList.get(size-2));
            succ = hashTable.get(activeNodeList.get(0));;
        }else{
            pred = hashTable.get(activeNodeList.get(insertIndex-1));
            succ = hashTable.get(activeNodeList.get(insertIndex+1));
        }

        //for debugging
        activePortList.clear();
        for (String remoteHash : activeNodeList){
            activePortList.add(processMap.get(hashTable.get(remoteHash)));
        }

        return pred+":"+succ;
    }

    private String generatePredSucc(String port){

        String portHash = null;
        try{
            portHash = genHash(port);
        }catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }

        int size = activeNodeList.size();
        int presentIndex = activeNodeList.indexOf(portHash);
        String pred;
        String succ;
        if(size==1){
            pred = port;
            succ = port;
        }else if(presentIndex == 0){
            succ = hashTable.get(activeNodeList.get(1));
            pred = hashTable.get(activeNodeList.get(size-1));
        }else if(presentIndex == size-1){
            pred = hashTable.get(activeNodeList.get(size-2));
            succ = hashTable.get(activeNodeList.get(0));;
        }else{
            pred = hashTable.get(activeNodeList.get(presentIndex-1));
            succ = hashTable.get(activeNodeList.get(presentIndex+1));
        }
        return pred+":"+succ;
    }



























}
