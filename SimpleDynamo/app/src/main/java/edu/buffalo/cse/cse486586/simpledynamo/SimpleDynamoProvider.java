package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.StreamCorruptedException;
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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

public class SimpleDynamoProvider extends ContentProvider {

	static final String[] REMOTE_PORT = {"11108","11112","11116", "11120", "11124"};
	static final int SERVER_PORT = 10000;
	static final String TAG = "SimpleDynamo";
	static final String providerUri = "content://edu.buffalo.cse.cse486586.simpledynamo.provider";
	String delimiter = "<GMP>";
	String portStr;
	String myPortNumber;

	ArrayList<String> activeNodeList = new ArrayList<String>();
	ArrayList<String> successorList = new ArrayList<String>();
	ArrayList<String> nodeList = new ArrayList<String>();

	private HashMap<String, String> hashTable = new HashMap<String, String>();
	private HashMap<String,String> deadNodeBuffer = new HashMap<String, String>();


	String succ1;
	String succ2;

	String myHash;
	String succ1Hash;
	String succ2Hash;

	Object lock = new Object();
	String failedPort = null;


	ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	ReadWriteLock nodeListLock = new ReentrantReadWriteLock();


	String deleteRequest = "DELETE";
	String deleteAck = "DELETE_ACK";
	String queryRequest = "QUERY";
	String queryAck = "QUERY_ACK";
	String insert = "INSERT";
	String replicate = "REPLICATE";
	String insertAck = "INSERT_ACK";
	String replicateAck = "REPLICATE_ACK";
	String recoveryRequest = "RECOVERY";
	String recoveryAck= "RECOVERY_ACK";
	Boolean insertOverride = false;
	Boolean sendToSuccessor = false;
	Boolean queryOverride = false;
	String sourcePortString = "0";

	Boolean isInserting = false;


	SimpleDynamoDatabase dbHelper;
	SQLiteDatabase dbWrite;
	SQLiteDatabase dbRead;

	//*************************************************************************** ON CREATE ***************************************************************************

	@Override
	public boolean onCreate() {
		initializations();
		failedPort = null;
		return false;
	}


	//*************************************************************************** DELETE ***************************************************************************
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		dbRead.delete(SimpleDynamoContract.SimpleDynamoDB.TABLE_NAME,null,null);

		String msgHash = null;
		String msgToSend;

		if(selection.equals("@")){
			Log.i("DELETE","LOCAL DELETE REQUEST hence deleting all keys locally");
			dbRead.delete(SimpleDynamoContract.SimpleDynamoDB.TABLE_NAME,null,null);

		}
		else if(selection.equals("*")){
			Log.i("DELETE","GLOBAL DELETE REQUEST hence deleting here and asking all nodes to delete keys");
			dbRead.delete(SimpleDynamoContract.SimpleDynamoDB.TABLE_NAME,null,null);
			deleteFromAllNodes();

		}else {
			Log.i("DELETE","Key to be deleted is "+selection);
			try {
				msgHash = genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			deleteSpecificKey(msgHash,selection);
		}
		return 0;
	}



	//*************************************************************************** INSERT ***************************************************************************
	@Override
	public Uri insert(Uri uri, ContentValues values) {

		String msgHash = null;
		String msgToSend;

		try{
			Log.i("INSERT","Entered Insert in port "+portStr);
			msgHash = genHash(values.getAsString("key"));
			String coOrdinator = getCoordinator(msgHash);
			Log.i("INSERT","Coordinator for key "+values.getAsString("key")+" is "+coOrdinator);


		 	if(coOrdinator.equals(portStr)){

				Log.i("INSERT","Current node is Co-ordinator for key "+ values.getAsString("key") + "hence inseting locally");
				dbWrite.insertWithOnConflict(SimpleDynamoContract.SimpleDynamoDB.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
				Log.i("INSERT","Replicating for key "+ values.getAsString("key")+" in next function");
				replicateInSuccessors(values,portStr);
				return uri;


			}else{


				nodeListLock.writeLock().lock();
				nodeList.add(coOrdinator);
				String successors = generateSuccessors(coOrdinator);
				nodeList.add(successors.split(":")[0]);
				nodeList.add(successors.split(":")[1]);

				for(String port: nodeList){

					if((failedPort!=null) && (port.equals(failedPort))){
						Log.i("INSERT","DEAD PORT hence entering "+values.getAsString("key")+" in deadBufferList");
						deadNodeBuffer.put(values.getAsString("key"),values.getAsString("value"));
						continue;
					}

					msgToSend = values.getAsString("key")+":"+values.getAsString("value")+delimiter+ insert +delimiter+portStr+delimiter+port;
					String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend).get();
					if(response.equals("Done")){
						Log.i("INSERT",values.getAsString("key")+" successfully inserted in "+port+" and replicated in its successors");
					}
				}

				nodeList.clear();
				nodeListLock.writeLock().unlock();

				return uri;
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return uri;
	}


	//*************************************************************************** QUERY ***************************************************************************
	@Override
	public  Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		try {


			if (selection.equals("@")) {
				Cursor cursor;
				readWriteLock.readLock().lock();
				cursor = dbRead.rawQuery("select * from " + SimpleDynamoContract.SimpleDynamoDB.TABLE_NAME, null);
				readWriteLock.readLock().unlock();
				Log.i("QUERY", "CURSOR TO STRING looks like " + cursor.toString());
				return cursor;
			}

			else if (selection.equals("*")) {
				Cursor cursor;

				MatrixCursor returnCursor = new MatrixCursor(new String[]{"key", "value"});
				readWriteLock.readLock().lock();
				cursor = dbRead.rawQuery("select * from " + SimpleDynamoContract.SimpleDynamoDB.TABLE_NAME, null);
				readWriteLock.readLock().unlock();
				cursor.moveToFirst();
				String msgToSend;

				Log.i("QUERY","Adding own rows to cursor");
				while (!cursor.isAfterLast()) {
					returnCursor.newRow().add("key",cursor.getString(0)).add("value",cursor.getString(1));
					cursor.moveToNext();
				}


				Log.i("QUERY","Asking for data from other ports");
				for (String portHash : activeNodeList) {

					String port = hashTable.get(portHash);

					if((failedPort!=null) && (port.equals(failedPort))){
						continue;
					}

					msgToSend = "NULL" + delimiter + queryRequest + delimiter + portStr + delimiter + port;


					Log.i("QUERY","Asking for data from port "+port);
					String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend).get();
					if ((!response.equals("null"))&&(response != null)) {
						String[] valuesArray = response.split(":");
						Log.i("QUERY", "Got back data from port " + port);
						for (String dataPoint : valuesArray) {
							if (dataPoint.equals("")) {
								continue;
							}
							String key = dataPoint.split(",")[0];
							String value = dataPoint.split(",")[1];
							returnCursor.newRow().add("key", key).add("value", value);
						}
					}
				}
				return returnCursor;

			}

			else{
				Log.i("QUERY","Query request for single element "+ selection);
				Cursor cursor;
				String msgToSend;
				MatrixCursor returnCursor = new MatrixCursor(new String[]{"key", "value"});
				String coOrdinator = getCoordinator(genHash(selection));


				if(coOrdinator.equals(portStr)){
					Log.i("QUERY","FOUND : Selection parameter is locally available because current node is co-ordniator "+selection);
					selectionArgs = new String[] {selection};
					selection = SimpleDynamoContract.SimpleDynamoDB.Key + " = ?";

					readWriteLock.readLock().lock();

					cursor = dbRead.query(SimpleDynamoContract.SimpleDynamoDB.TABLE_NAME,   // The table to query
							null,             // The array of columns to return (pass null to get all)
							selection,              // The columns for the WHERE clause
							selectionArgs,          // The values for the WHERE clause
							null,                   // don't group the rows
							null,                   // don't filter by row groups
							null               // The sort order
					);

					readWriteLock.readLock().unlock();
					return cursor;
				}

				else{

					nodeListLock.writeLock().lock();
					nodeList.add(coOrdinator);
					String successors = generateSuccessors(coOrdinator);
					nodeList.add(successors.split(":")[0]);
					nodeList.add(successors.split(":")[1]);

					for(String port: nodeList) {


						if((failedPort!=null) && (port.equals(failedPort))){
							continue;
						}


						msgToSend = selection + delimiter + queryRequest + delimiter + portStr + delimiter + port;
						Log.i("QUERY", " Current node not co-ordinator Asking for Single Query from port " + port);
						String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend).get();
						if ((!response.equals("null"))&&(response != null)) {
							returnCursor = new MatrixCursor(new String[]{"key", "value"});
							String key = response.split(",")[0];
							String value = response.split(",")[1];
							returnCursor.newRow().add("key", key).add("value", value);
							break;
						}
					}

					nodeList.clear();
					nodeListLock.writeLock().unlock();
					return returnCursor;
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}


	//*************************************************************************** SERVER TASK ***************************************************************************


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];
			Socket serSocket = null;
			Socket portBroadcastSocket;

			try {

				while (true){

					Log.i("SERVER PORT","My Port : "+portStr);
					Log.i("SERVER PORT","My Succ1 : "+succ1);
					Log.i("SERVER PORT","My Succ2 : "+succ2);
					serSocket = serverSocket.accept();

					ObjectInputStream inMessage = new ObjectInputStream(serSocket.getInputStream());
					String messageReceived = (String) inMessage.readObject();
					String[] messageReceivedArray = messageReceived.split(delimiter);
					Log.i("SERVER","MESSAGE RECEIVED : "+messageReceived);

					if(messageReceivedArray[1].equals(replicate)){

						ContentValues values = new ContentValues();
						values.put("key",messageReceivedArray[0].split(":")[0]);
						values.put("value",messageReceivedArray[0].split(":")[1]);

						readWriteLock.writeLock().lock();
						dbWrite.insertWithOnConflict(SimpleDynamoContract.SimpleDynamoDB.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
						readWriteLock.writeLock().unlock();

						Log.i("SERVER","The key "+messageReceivedArray[0].split(":")[0]+" has been replicated");
						String messageToSend = "NULL"+delimiter+replicateAck+delimiter+portStr+delimiter+messageReceivedArray[2];
						ObjectOutputStream outMessage = new ObjectOutputStream(serSocket.getOutputStream());
						outMessage.writeObject(messageToSend);
						Log.i("SERVER",  "REPLICATE ACK");
						outMessage.flush();
						serSocket.close();
					}

					else if(messageReceivedArray[1].equals(insert)){

						ContentValues values = new ContentValues();
						values.put("key",messageReceivedArray[0].split(":")[0]);
						values.put("value",messageReceivedArray[0].split(":")[1]);


						readWriteLock.writeLock().lock();
						dbWrite.insertWithOnConflict(SimpleDynamoContract.SimpleDynamoDB.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
						readWriteLock.writeLock().unlock();

						Log.i("SERVER","The key "+messageReceivedArray[0].split(":")[0]+" has inserted");
						String messageToSend = "Done";
						ObjectOutputStream outMessage = new ObjectOutputStream(serSocket.getOutputStream());
						outMessage.writeObject(messageToSend);
						Log.i("SERVER",  "Inserted into current and replicated");
						outMessage.flush();
						serSocket.close();
					}


					else if(messageReceivedArray[1].equals(queryRequest)) {
						Cursor cursor;
						String msg = "";
						String key;
						String value;

						Log.i("SERVER","QUERY REQUEST FOR SELECTION "+messageReceivedArray[0]);

						if (messageReceivedArray[0].equals("NULL")){

							readWriteLock.readLock().lock();
							cursor = dbRead.rawQuery("select * from " + SimpleDynamoContract.SimpleDynamoDB.TABLE_NAME, null);
							readWriteLock.readLock().unlock();

							Log.i("QUERY", "CURSOR : got  " + cursor.getCount() + "values");
							for (cursor.moveToFirst();!cursor.isAfterLast();cursor.moveToNext()) {
								key = cursor.getString(0);
								value = cursor.getString(1);
								msg = msg+key+","+value+":";
							}
							cursor.close();

						}

						else {

							readWriteLock.readLock().lock();
							String selection = messageReceivedArray[0];
							String [] selectionArgs = new String[] {selection};
							selection = SimpleDynamoContract.SimpleDynamoDB.Key + " = ?";
							cursor = dbRead.query(SimpleDynamoContract.SimpleDynamoDB.TABLE_NAME,   // The table to query
									null,             // The array of columns to return (pass null to get all)
									selection,              // The columns for the WHERE clause
									selectionArgs,          // The values for the WHERE clause
									null,                   // don't group the rows
									null,                   // don't filter by row groups
									null               // The sort order
							);
							readWriteLock.readLock().unlock();

							Log.i("QUERY","CURSOR : got  "+cursor.getCount() + "values");
							cursor.moveToFirst();
							key = cursor.getString(0);
							value = cursor.getString(1);
							msg = key+","+value;
							Log.i("SERVER","retrieved "+msg);
						}

						ObjectOutputStream outMessage = new ObjectOutputStream(serSocket.getOutputStream());
						outMessage.writeObject(msg);
						Log.i("SERVER",  "QUERY_ACK : Sending retrieved result back to +"+messageReceivedArray[2]);
						outMessage.flush();
						serSocket.close();
					}
					else if(messageReceivedArray[1].equals(recoveryRequest)){
						String msg = "";
						String key;
						String value;


						Log.i("SERVER", " RECOVERY Adding values from dead node buffer to message");
						if(deadNodeBuffer.size()==0){
							msg="null";
						}
						else{
							for (Map.Entry pair : deadNodeBuffer.entrySet()) {
								key = (String) pair.getKey();
								value = (String) pair.getValue();
								msg = msg+key+","+value+":";
							}
							deadNodeBuffer.clear();
							failedPort = null;
						}

						ObjectOutputStream outMessage = new ObjectOutputStream(serSocket.getOutputStream());
						outMessage.writeObject(msg);
						Log.i("SERVER",  "RECOVERY_ACK : Sending retrieved result back to +"+messageReceivedArray[2]);
						outMessage.flush();
						serSocket.close();
					}else if(messageReceivedArray[1].equals(deleteRequest)){
						dbRead.delete(SimpleDynamoContract.SimpleDynamoDB.TABLE_NAME,null,null);
						serSocket.close();
					}
				}
			}catch (Exception e){
				e.printStackTrace();
			}
			return null;
		}
	}

	//*************************************************************************** CLIENT TASK ***************************************************************************


	private class ClientTask extends AsyncTask<String, Void, String> {

		Socket socket;
		String[] messageArray;

		@Override
		protected String doInBackground(String... msgs) {
			try{
				 messageArray = msgs[0].split(delimiter);

				//sending replication request
				if(messageArray[1].equals(replicate)){
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(messageArray[3])*2);
					socket.setSoTimeout(500);
					ObjectOutputStream outMessage = new ObjectOutputStream(socket.getOutputStream());
					outMessage.writeObject(msgs[0]);
					Log.i("CLIENT","REPLICATE : Sending Replicate Request to port "+messageArray[3]);
					outMessage.flush();

					ObjectInputStream inMessage = new ObjectInputStream(socket.getInputStream());
					String messageReceived = (String) inMessage.readObject();
					String[] receivedMessageArray = messageReceived.split(delimiter);
					Log.i("CLIENT","REPLICATE_ACK : Received");
					socket.close();
					return "Done";

				}

				else if(messageArray[1].equals(insert)){
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(messageArray[3])*2);
					socket.setSoTimeout(500);
					ObjectOutputStream outMessage = new ObjectOutputStream(socket.getOutputStream());
					outMessage.writeObject(msgs[0]);
					Log.i("CLIENT","INSERT : Sending Insert Request for key "+messageArray[0].split(":")[0] +" to port "+messageArray[3]);
					outMessage.flush();

					ObjectInputStream inMessage = new ObjectInputStream(socket.getInputStream());
					String messageReceived = (String) inMessage.readObject();
					String[] receivedMessageArray = messageReceived.split(delimiter);
					Log.i("CLIENT","INSERT_ACK : Received");
					socket.close();
					return "Done";

				}

				else if(messageArray[1].equals(queryRequest)) {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(messageArray[3]) * 2);
						socket.setSoTimeout(500);
						ObjectOutputStream outMessage = new ObjectOutputStream(socket.getOutputStream());
						outMessage.writeObject(msgs[0]);
						Log.i("CLIENT", "Asking for query from " + messageArray[3]);
						outMessage.flush();


						//Receiving Query Response
						Log.i("CLIENT", "Waiting for response from server for response to selection " + messageArray[0]);
						ObjectInputStream inMessage = new ObjectInputStream(socket.getInputStream());
						String messageReceived = (String) inMessage.readObject();
						Log.i("CLIENT", "Got reponse from server " + messageReceived);
						socket.close();
						return messageReceived;
				}

				else if(messageArray[1].equals(recoveryRequest)) {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(messageArray[3]) * 2);
					socket.setSoTimeout(500);
					ObjectOutputStream outMessage = new ObjectOutputStream(socket.getOutputStream());
					outMessage.writeObject(msgs[0]);
					Log.i("CLIENT", "Asking for buffer from " + messageArray[3]);
					outMessage.flush();


					//Receiving Query Response
					Log.i("CLIENT", "Waiting for response from server for response to selection " + messageArray[0]);
					ObjectInputStream inMessage = new ObjectInputStream(socket.getInputStream());
					String messageReceived = (String) inMessage.readObject();
					Log.i("CLIENT", "Got reponse from server " + messageReceived);
					socket.close();
					return messageReceived;
				}
				else if(messageArray[1].equals(deleteRequest)){
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(messageArray[3]) * 2);
					socket.setSoTimeout(500);
					ObjectOutputStream outMessage = new ObjectOutputStream(socket.getOutputStream());
					outMessage.writeObject(msgs[0]);
					Log.i("CLIENT", "Delete request send to " + messageArray[3]);
					outMessage.flush();
					socket.close();
				}

			} catch (OptionalDataException e) {
				e.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (StreamCorruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
				failedPort = socket.getPort()+"";
				if((messageArray[1].equals(insert)) || (messageArray[1].equals(replicate))){
					Log.i("CLIENT IO EXCEPTION","ADDED "+messageArray[0].split(":")[0]+" to dead node buffer");
					deadNodeBuffer.put(messageArray[0].split(":")[0],(messageArray[0].split(":")[1]));
				}

				try {
					socket.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			return "null";

		}

	}




    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}
	//*************************************************************************** HELPERS ***************************************************************************



	private void initializations(){
		try {

			Context context = getContext();
			TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
			portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			myPortNumber = String.valueOf((Integer.parseInt(portStr) * 2));

			Log.i("OnCreate","My Port String "+portStr);

			myHash = genHash(portStr);
			hashTable.put(genHash("5554"), "5554");
			hashTable.put(genHash("5556"), "5556");
			hashTable.put(genHash("5558"), "5558");
			hashTable.put(genHash("5560"), "5560");
			hashTable.put(genHash("5562"), "5562");


			for (String remotePort : REMOTE_PORT){
				int portNo = Integer.parseInt(remotePort)/2;
				addNodesIntoActiveList(portNo+"");
			}

			String nextNodes = generateSuccessors(portStr);
			succ1 = nextNodes.split(":")[0];
			succ2 = nextNodes.split(":")[1];

			succ1Hash = genHash(succ1);
			succ2Hash = genHash(succ2);


			dbHelper = new SimpleDynamoDatabase(getContext());
			dbWrite = dbHelper.getWritableDatabase();
			dbRead = dbHelper.getReadableDatabase();

			successorList.add(succ1);
			successorList.add(succ2);

			try {
				ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
				new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			} catch (IOException e) {
				//Log.e(TAG, "Can't create a ServerSocket");
			}



			// RECOVERING FROM DEAD NODE
			for (String portHash : activeNodeList) {


				String port = hashTable.get(portHash);

				if(port.equals(portStr)){
					continue;
				}

				String msgToSend = "NULL" + delimiter + recoveryRequest + delimiter + portStr + delimiter + port;

				Log.i("RECOVERY","Asking for data from port " + port);
				String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend).get();
				if (!response.equals("null")){
					String [] valuesArray = response.split(":");
					Log.i("RECOVERY","Got back data from port " + port);


					ContentValues values = new ContentValues();
					for ( String dataPoint : valuesArray ) {
						if(dataPoint.equals("")){
							continue;
						}
						String key = dataPoint.split(",")[0];
						String value = dataPoint.split(",")[1];

						values.put("key",key);
						values.put("value",value);
						dbWrite.insertWithOnConflict(SimpleDynamoContract.SimpleDynamoDB.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
					}
				}
			}


		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}



	private String getCoordinator(String msgHash){

		for (String activeNodeHash: activeNodeList){

			if (msgHash.compareTo(activeNodeHash)<0){
					return hashTable.get(activeNodeHash);
			}
			else{
				if(activeNodeList.indexOf(activeNodeHash) == activeNodeList.size()-1){
					Log.i("CO-ORD","msgHash is greater than last node in system, hence coordinator is 1st node");
					return hashTable.get(activeNodeList.get(0));
				}
			}
		}
		return null;
	}



	private void addNodesIntoActiveList(String port){

		String portHash = null;
		try{
			portHash = genHash(port);
		}catch (NoSuchAlgorithmException e){
			e.printStackTrace();
		}
		activeNodeList.add(portHash);
		Collections.sort(activeNodeList);


	}

	private String generateSuccessors(String port){

		String portHash = null;
		try{
			portHash = genHash(port);
		}catch (NoSuchAlgorithmException e){
			e.printStackTrace();
		}

		int size = activeNodeList.size();
		int presentIndex = activeNodeList.indexOf(portHash);
		String succ1;
		String succ2;
		if(presentIndex == 0){
			succ1 = hashTable.get(activeNodeList.get(1));
			succ2 = hashTable.get(activeNodeList.get(2));
		}else if(presentIndex == size-1){
			succ1 = hashTable.get(activeNodeList.get(0));
			succ2 = hashTable.get(activeNodeList.get(1));;
		}else if(presentIndex == size-2) {
			succ1 = hashTable.get(activeNodeList.get(size-1));
			succ2 = hashTable.get(activeNodeList.get(0));;
		}else{
			succ1 = hashTable.get(activeNodeList.get(presentIndex+1));
			succ2 = hashTable.get(activeNodeList.get(presentIndex+2));
		}
		return succ1+":"+succ2;
	}

	private void replicateInSuccessors(ContentValues values, String origin) throws ExecutionException, InterruptedException {
		String key = (String) values.get("key");
		String value = (String) values.get("value");

		String response;
		for(String port: successorList){
			String msgToSend = key+":"+value+delimiter+replicate+delimiter+origin+delimiter+port;
			Log.i("REPLICATION","Replicating key "+key+" in port "+port);

			response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend).get();

			if(response.equals("Done")){
				Log.i("REPLICATION","Replication done for "+key+" in port "+port);
			}
		}

	}


	private void deleteFromAllNodes() {

		for (String portHash : activeNodeList) {

			String port = hashTable.get(portHash);

			if((failedPort!=null) && (port.equals(failedPort))){
				continue;
			}

			if(port.equals(portStr)){
				continue;
			}
			String msgToSend = "NULL" + delimiter + deleteRequest + delimiter + portStr + delimiter + port;
			Log.i("DELETE FUNCTION","Deleting from port "+port);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);

		}
	}


	private void deleteSpecificKey(String msgHash, String selection) {

		String coOrdinator = getCoordinator(msgHash);
		nodeListLock.writeLock().lock();
		nodeList.add(coOrdinator);
		String successors = generateSuccessors(coOrdinator);
		nodeList.add(successors.split(":")[0]);
		nodeList.add(successors.split(":")[1]);
		dbRead.delete(SimpleDynamoContract.SimpleDynamoDB.TABLE_NAME,null,null);
		nodeList.clear();
		nodeListLock.writeLock().unlock();

	}



}