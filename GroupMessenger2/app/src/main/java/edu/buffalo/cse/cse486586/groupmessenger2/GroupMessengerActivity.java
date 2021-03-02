package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String[] REMOTE_PORT = {"11108","11112","11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String providerUri = "content://edu.buffalo.cse.cse486586.groupmessenger2.provider";
    int seqNumber = 0;
    int proposalNumber = 0;
    int finalProposalNumber = 0;

    int failedPort = 0;
    int failedProcess = 0;
    int portBroadcastFlag = 0;

    String myPortNumber;
    String delimiter = "<GMP>";
    String proposalRequest = "PROPOSAL_REQUEST";
    String proposalReguestACK = "PROPOSAL_ACK";
    String finalProposal = "FINAL_PROPOSAL";
    String finalProposalACK = "FINAL_PROPOSAL_ACK";



    HashMap<Integer,Integer> portMap;
    Comparator<String> qComparator = new queueComparator();
    PriorityQueue<String> serverQueue =new PriorityQueue<String>(25, qComparator);


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        //Creating the server socket
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        portMap= new HashMap<Integer, Integer>();
        portMap.put(11108,1);
        portMap.put(11112,2);
        portMap.put(11116,3);
        portMap.put(11120,4);
        portMap.put(11124,5);

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPortNumber = String.valueOf((Integer.parseInt(portStr) * 2));
        //Log.d("one More---",myPortNumber);

        final EditText editText = (EditText) findViewById(R.id.editText1);

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        final Button sendButton = (Button) findViewById(R.id.button4);

        sendButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                Log.d("send Button", "Send Button Clicked");
                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            }
        });


    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }




// ******************************************************* SERVER ********************************************************************
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];


            Socket serSocket=null;
            String messageReceived;
            String messageToSend;
            String messageID;
            String messageToEnqueue;
            Socket portBroadcastSocket = null;


            try {
                while (true) {
                    //accept to listen to the port
                    Log.i("FAILED PORT IN SERVER", failedPort + "");

                    if (failedProcess != 0 && portBroadcastFlag == 0) {
                        for (String remotePort : REMOTE_PORT) {
                            try {

                                if (Integer.parseInt(remotePort) == failedPort || Integer.parseInt(remotePort) == Integer.parseInt(myPortNumber)) {
                                    continue;
                                }

                                portBroadcastSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                                messageToSend = failedPort + delimiter + failedProcess + delimiter + "PORT_FAILURE_NOTIFICATION";


                                ObjectOutputStream outMessage = new ObjectOutputStream(portBroadcastSocket.getOutputStream());
                                outMessage.writeObject(messageToSend);
                                Log.e("Sending FPORT:", "Sending Failed Port " + failedPort + "to port " + portMap.get(Integer.parseInt(remotePort)));
                                outMessage.flush();
                                portBroadcastFlag = 1;
                                portBroadcastSocket.close();

                            } catch (Exception e) {
                                e.printStackTrace();
                                portBroadcastSocket.close();
                            }
                        }
                    }

                    if(portBroadcastFlag==1){
                        Iterator itr = serverQueue.iterator();
                        while (itr.hasNext()) {
                            String temp = (String) itr.next();
                            String[] tempArray = temp.split(delimiter);
                            String id = tempArray[1];
                            String finalFlag = tempArray[4];
                            int processNumber = Integer.parseInt(id.split(":")[1]);

                            if (processNumber == failedProcess && finalFlag.equals("0") ) {
                                Log.i("Deleting after FPORT", "Currently in Queue " + id + "->" + tempArray[3] + "." + tempArray[2]);
                                itr.remove();
                            }
                        }
                    }

                    String first_element = serverQueue.peek();
                    while (serverQueue.size() != 0 && first_element.split(delimiter)[4].equals("1")) {
                        String messageToDeliver = serverQueue.poll();
                        Log.i("Server Delivering", messageToDeliver.split(delimiter)[0] + " -->" + messageToDeliver.split(delimiter)[1]);
                        publishProgress(messageToDeliver.split(delimiter)[0]);
                        first_element = serverQueue.peek();
                    }


//                    deleteFromServerQueue(failedProcess);
                    serSocket = serverSocket.accept();
                    ObjectInputStream inMessage = new ObjectInputStream(serSocket.getInputStream());
                    messageReceived = (String) inMessage.readObject();
                    String[] messageArray = messageReceived.split(delimiter);

//                    if (messageArray.length == 2 && messageArray[0].equals("Heart")) {
////                        ObjectOutputStream outMessage = new ObjectOutputStream(portBoradcastSocket.getOutputStream());
////                        outMessage.writeObject(messageToSend);
////                        messageToSend = "Alicw";
//                        continue;
//
//                    }

                    if (messageArray.length == 3 && messageArray[2].equals("PORT_FAILURE_NOTIFICATION")) {
                        failedPort = Integer.parseInt(messageArray[0]);
                        failedProcess = Integer.parseInt(messageArray[1]);
                        portBroadcastFlag = 1;
                        Log.i("Changed FPORT:", "Changing Failed port after receiving from Broadcast");

                        Iterator itr = serverQueue.iterator();
                        while (itr.hasNext()) {
                            String temp = (String) itr.next();
                            String[] tempArray = temp.split(delimiter);
                            String id = tempArray[1];
                            String finalFlag = tempArray[4];
                            int processNumber = Integer.parseInt(id.split(":")[1]);

                            if (processNumber == failedProcess && finalFlag.equals("0") ) {
                                Log.i("Deleting after FPORT", "Currently in Queue " + id + "->" + tempArray[3] + "." + tempArray[2]);
                                itr.remove();
                            }
                        }
                    }
                    else if (messageArray[5].equals(proposalRequest)) {
                        messageID = messageArray[1] + ":" + messageArray[2];
                        int srcProcessNumber = Integer.parseInt(messageID.split(":")[1]);

                        ObjectOutputStream outMessage = new ObjectOutputStream(serSocket.getOutputStream());
                        proposalNumber += 1;
                        messageToSend = messageArray[0] + delimiter + messageArray[1] + delimiter + messageArray[2] + delimiter + portMap.get(Integer.parseInt(myPortNumber)) + delimiter + proposalNumber + delimiter + proposalReguestACK;

                        messageToEnqueue = messageArray[0] + delimiter + messageID + delimiter + portMap.get(Integer.parseInt(myPortNumber)) + delimiter + proposalNumber + delimiter + "0";

                        if (srcProcessNumber != failedProcess) {
                            serverQueue.add(messageToEnqueue);
                            Log.i("SERVER QUEUE ADD", messageArray[0] + "->" + proposalNumber + "." + portMap.get(Integer.parseInt(myPortNumber)));
                            outMessage.writeObject(messageToSend);
                            outMessage.flush();
                            serSocket.close();
                        }

                    } else if (messageArray[5].equals(finalProposal)) {
                        messageID = messageArray[1] + ":" + messageArray[2];
                        int srcProcessNumber = Integer.parseInt(messageID.split(":")[1]);
                        Log.i("Final Proposal Received", messageArray[0] + " ->" + messageArray[4] + "." + messageArray[3] + "");

                        if (Integer.parseInt(messageArray[4]) >= proposalNumber) {
                            proposalNumber = Integer.parseInt(messageArray[4]);
                        }

                        Iterator itr = serverQueue.iterator();
                        while (itr.hasNext()) {
                            String temp = (String) itr.next();
                            String[] tempArray = temp.split(delimiter);
                            String id = tempArray[1];
                            String finalFlag = tempArray[4];
                            int processNumber = Integer.parseInt(id.split(":")[1]);

                            if (id.equals(messageID)) {
                                Log.i("Deleting From Queue", "Final Proposal " + messageArray[0] + " ->" + messageArray[4] + "." + messageArray[3] + "<------>" + "Currently in Queue " + messageID + "->" + tempArray[3] + "." + tempArray[2]);
                                itr.remove();
                            }


                            if (processNumber == failedProcess && finalFlag.equals("0") ) {
                                Log.i("Deleting For Failed Prt", "Currently in Queue " + messageID + "->" + tempArray[3] + "." + tempArray[2]);
                                itr.remove();
                            }

                        }

                        if (srcProcessNumber != failedProcess) {
                            messageToEnqueue = messageArray[0] + delimiter + messageID + delimiter + messageArray[3] + delimiter + messageArray[4] + delimiter + "1";
                            serverQueue.add(messageToEnqueue);
                            Log.i("Queue after Final ", serverQueue.toString());
                        }

                    }

                    first_element = serverQueue.peek();
                    while (serverQueue.size() != 0 && first_element.split(delimiter)[4].equals("1")) {
                        String messageToDeliver = serverQueue.poll();
                        Log.i("Server Delivering", messageToDeliver.split(delimiter)[0] + " -->" + messageToDeliver.split(delimiter)[1]);
                        publishProgress(messageToDeliver.split(delimiter)[0]);
                        first_element = serverQueue.peek();
                    }
                }
            } catch (IOException e) {
                Log.e(TAG, "Server I/O exception");
                failedPort=serSocket.getPort();
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                Log.e(TAG, "ClassNotFound Exception");
            }

            return null;
        }


//        public void broadcast(String type){
//            Iterator itr = serverQueue.iterator();
//            while(itr.hasNext()){
//                String wmsg= (String)itr.next();
//                int sPort = Integer.parseInt(wmsg.split(delimiter)[1].split(":")[1]);
//                if(sPort == failedPort) {
//                    Log.d("Printing Source Failed",sPort+"--->"+wmsg.split(delimiter)[1]);
//                    Log.d(TAG,"Removing failed port message :"+wmsg);
//                    itr.remove();
//                }
//            }
//        }



        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
//            Log.d("str",strReceived);

            ContentValues keyValuesToInsert = new ContentValues();
            keyValuesToInsert.put("key", seqNumber);
            keyValuesToInsert.put("value", strReceived);
            seqNumber++;
//            Log.d("uri",providerUri);
//            Log.d("uri", String.valueOf(keyValuesToInsert));
            Uri newUri;
            newUri =  getContentResolver().insert(Uri.parse(providerUri), keyValuesToInsert);

            TextView textView = (TextView) findViewById(R.id.textView1);
            textView.append(strReceived + "\t\n");
            return;
        }
    }


// **************************************************************** CLIENT **********************************************************************

    private class ClientTask extends AsyncTask<String, Void, Void> {

        Socket socket;
        Socket finalProposalSocket;
        String messageReceived;

        String messageToSend;

        int finalProcessNumber;

        int currentProcessNumber;
        int sourceProcessNumber;
        int currentProposal;

        int initialSeqNumber;



        @Override
        protected Void doInBackground(String... msgs) {

            currentProposal = proposalNumber+1;
            sourceProcessNumber = portMap.get(Integer.parseInt(myPortNumber));
            currentProcessNumber = portMap.get(Integer.parseInt(myPortNumber));
            initialSeqNumber = proposalNumber+1;

            for (String remotePort : REMOTE_PORT) {
//                Log.e("Client", remotePort);

                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                    messageToSend = msgs[0];


                    //First time asking for proposal
                    messageToSend = messageToSend+delimiter+initialSeqNumber+delimiter+sourceProcessNumber+delimiter+currentProcessNumber+delimiter+"NULL"+delimiter+proposalRequest;
                    ObjectOutputStream outMessage = new ObjectOutputStream(socket.getOutputStream());
                    outMessage.writeObject(messageToSend);
                    Log.i("Client Side","Asking for proposal for message "+msgs[0]+" from port "+socket.getPort());
                    outMessage.flush();


                    //Receiving proposal
                    ObjectInputStream inMessage = new ObjectInputStream(socket.getInputStream());
                    messageReceived = (String) inMessage.readObject();
                    String [] messageArray = messageReceived.split(delimiter);
                    Log.i("Client Side","Proposal message "+messageToSend+" from port "+socket.getPort());
                    socket.close();


                    int receivedProposal = Integer.parseInt(messageArray[4]);
                    int receivedFromProcess = Integer.parseInt(messageArray[3]);

                    if(currentProposal<receivedProposal) {
                        currentProposal = receivedProposal;
                        currentProcessNumber = receivedFromProcess;
                    }

                    else if(currentProposal == receivedProposal){
                        if(currentProcessNumber <receivedFromProcess) {
                            currentProposal = receivedProposal;
                            currentProcessNumber = receivedFromProcess;
                        }
                    }

                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException at port: "+socket.getPort());
                    e.printStackTrace();
                    failedPort = socket.getPort();
                    failedProcess = portMap.get(failedPort);
                    try {
                        socket.close();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }

                } catch (ClassNotFoundException e){
                    Log.e(TAG,"Class Not Found Exception");
                    try {
                        socket.close();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }
            }

            finalProposalNumber = currentProposal;
            finalProcessNumber = currentProcessNumber;


//            Log.d("Final Proposal",finalProposalNumber+"."+finalProcessNumber+"");

            for (String remotePort : REMOTE_PORT) {
                try {
                    Log.i("CLIENT final prposal","Sending to Port "+remotePort );
                    finalProposalSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));

                    String [] messageArray = messageReceived.split(delimiter);
                    messageToSend = messageArray[0]+delimiter+messageArray[1]+delimiter+messageArray[2]+delimiter+finalProcessNumber+delimiter+finalProposalNumber+delimiter+finalProposal;


                    ObjectOutputStream outMessage = new ObjectOutputStream(finalProposalSocket.getOutputStream());
                    outMessage.writeObject(messageToSend);
                    Log.i("Client Final Proposal ","Sending Final Proposal for message "+messageArray[0]+" "+finalProposalNumber+"."+finalProcessNumber+" to port "+portMap.get(Integer.parseInt(remotePort)));
                    outMessage.flush();
                    finalProposalSocket.close();

                    //read acknowledgement again

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                    try {
                        finalProposalSocket.close();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException during final proposal loop at port "+socket.getPort());
                    failedPort = finalProposalSocket.getPort();
                    failedProcess = portMap.get(failedPort);
                    e.printStackTrace();
                    try {
                        finalProposalSocket.close();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }

                }

            }

            return null;
        }
    }



    class queueComparator implements Comparator <String> {

        @Override
        public int compare(String s1,String s2){

            String [] messageArray1 = s1.split(delimiter);
            String [] messageArray2 = s2.split(delimiter);

            if(Integer.parseInt(messageArray1[3]) < Integer.parseInt(messageArray2[3])){
                return -1;
            }

            else if (Integer.parseInt(messageArray1[3]) > Integer.parseInt(messageArray2[3])){
                return 1;
            }
            else {

                if (Integer.parseInt(messageArray1[2]) < Integer.parseInt(messageArray2[2]) ){
                    return -1;
                }
                else if (Integer.parseInt(messageArray1[2]) > Integer.parseInt(messageArray2[2])){
                    return 1;
                }
            }
            return 0;
        }

    }

}
