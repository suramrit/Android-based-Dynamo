package edu.buffalo.cse.cse486586.simpledynamo;

import java.lang.reflect.Array;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.provider.MediaStore;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.apache.http.conn.ConnectTimeoutException;

public class SimpleDynamoProvider extends ContentProvider {
    public String my_hash;
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    public String myPort;
    String[] node_ports = {"11108", "11112", "11116", "11120","11124"}; // ports of nodes in the hash ring..
    private ArrayList<String> node_list = new ArrayList(Arrays.asList(node_ports));
    private ArrayList<String> node_hash_list = new ArrayList<String>();
    private ArrayList<String> local_key_list = new ArrayList<String>();
    private ArrayList<String> my_key_list = new ArrayList<String>();
    private ArrayList<String> succ_key_list = new ArrayList<String>();
    private ArrayList<String> succ2_key_list = new ArrayList<String>();
    private ArrayList<String> pred_key_list = new ArrayList<String>();
    private ArrayList<String> pred2_key_list = new ArrayList<String>();
    private Map<String, String> node_map = new TreeMap<String, String>();
    private Map<String, String> key_version_map = new HashMap<String, String>();
    static final int SERVER_PORT = 10000;
    static final int REQUEST_PORT = 11111;
    private String my_succ = new String();
    private String my_pred = new String();
    private String succ_node = new String();
    private String pred_node = new String();
    private String second_succ = new String();
    private String second_succ_node = new String();
    private String second_pred = new String();
    private String second_pred_node = new String();
    private ArrayList<String> neigh_list = new ArrayList<String>();
    private String min_hash = new String();
    private String max_hash = new String();
    private String min_hash_node = new String();
    private String max_hash_node = new String();
    private Boolean stand_alone = false;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        //check whether bellongs to me me or other nodes...

            local_key_list.remove(selection);
            pred_key_list.remove(selection);
            pred2_key_list.remove(selection);

        try {
            // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
            String filename = selection;
            getContext().deleteFile(selection);
        } catch (Exception e) {
            System.out.println("FILE Delete FAILED");
        }
            try{


            //pass to correct coordinator
            String correct_coordinator = new String();
            String coordinator_succ = new String();
            String coordinator_succ_node =  new String();
            String coordinator_succ2 = new String();
            String coordinator_succ2_node = new String();
            Log.e(TAG,"passing delete forward...");
            if(genHash(selection).compareTo(min_hash)<0 || genHash(selection).compareTo(max_hash)>0)
            {
                correct_coordinator = node_map.get(min_hash);
            }
            else{

                for(String hash : node_hash_list){
                    if(hash.compareTo(min_hash)!=0){
                        if(hash.compareTo(genHash(selection))>0){
                            correct_coordinator = node_map.get(hash);
                            break;
                        }
                    }
                }
            }

            Log.e(TAG,"correct cordinator for "+ genHash(selection)+" is: "+correct_coordinator +"..."+genHash(correct_coordinator));
            if((node_hash_list.indexOf(genHash(Integer.toString(Integer.parseInt(correct_coordinator)/2)))+1) == node_hash_list.size())
            {
                coordinator_succ = node_hash_list.get(0);
                coordinator_succ_node = node_map.get(coordinator_succ);
            }
            else
            {
                coordinator_succ = node_hash_list.get(node_hash_list.indexOf(genHash(Integer.toString(Integer.parseInt(correct_coordinator)/2)))+1);
                coordinator_succ_node = node_map.get(coordinator_succ);
            }

            if((node_hash_list.indexOf(coordinator_succ)+1) == node_hash_list.size())
            {
                coordinator_succ2 = node_hash_list.get(0);
                coordinator_succ2_node = node_map.get(coordinator_succ2);
            }
            else
            {
                coordinator_succ2 = node_hash_list.get(node_hash_list.indexOf(coordinator_succ)+1);
                coordinator_succ2_node = node_map.get(coordinator_succ2);
            }



            Log.e(TAG,"correct cordinator for "+ genHash(selection)+" is: "+correct_coordinator+"..."+genHash(correct_coordinator));

            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(correct_coordinator)); //
                ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                //obj_out_old.flush();
                String req_type = "6";
                Log.e(TAG, "Passing delete request: " + selection);
                Log.e(TAG, "comp:" + my_hash.compareTo(genHash(selection)) + "to:" + correct_coordinator);
                obj_out.writeObject(req_type);
                obj_out.writeObject(selection);

            }
            catch(IOException e){
                Log.e(TAG,"Error forwarding delete to :"+correct_coordinator+e);

            }
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(coordinator_succ_node)); //
                    ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                    //obj_out_old.flush();
                    String req_type = "6";
                    Log.e(TAG, "Passing delete request: " + selection);
                    Log.e(TAG, "comp:" + my_hash.compareTo(genHash(selection)) + "to:" + coordinator_succ_node);
                    obj_out.writeObject(req_type);
                    obj_out.writeObject(selection);

                }
                catch(IOException e)
                {
                    Log.e(TAG,"Error sending to coordinators sucessor"+e);
                }
                //SENDING TO SECOND SUCC..
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(coordinator_succ2_node)); //
                    ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                    //obj_out_old.flush();
                    String req_type = "6";
                    Log.e(TAG, "Passing delete request: " + selection);
                    Log.e(TAG, "comp:" + my_hash.compareTo(genHash(selection)) + "to:" + coordinator_succ_node);
                    obj_out.writeObject(req_type);
                    obj_out.writeObject(selection);

                }
                catch(IOException e)
                {
                    Log.e(TAG,"Error sending to coordinators sucessor"+e);
                }



        }
        catch(NoSuchAlgorithmException e){
            Log.e(TAG,"Error generating hash in delete");
        }
        Log.v("Deleted-", selection+"...");
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
            // TODO Auto-generated method stub
            Context context = getContext();
            Set<Map.Entry<String,Object>> s=values.valueSet();
            Iterator itr = s.iterator();
            String val = new String();
            String key = new String();
            String KEY_FIELD = "key";
            String VALUE_FIELD = "value";
            int i = 0;
            while(itr.hasNext())
            {
                Map.Entry me = (Map.Entry)itr.next();
                Object value =  me.getValue();
                if(i==0){
                    val = value.toString();
                    i++;
                }
                else
                {
                    key = value.toString();
                }
            }

            //got the key,value pair from insert request.. check node..
            try{

                if(my_hash.compareTo(min_hash)==0 && ( max_hash.compareTo(genHash(key))<0 || my_hash.compareTo(genHash(key))>0)){
                    //I will store it
                    Log.e(TAG, "MIN CASE: My Hash:" + my_hash + "..key hash:" + genHash(key) + "..comp:" + my_hash.compareTo(genHash(key)));
                    ContentValues val_insrt = new ContentValues();
                    val_insrt.put(VALUE_FIELD, val);
                    val_insrt.put(KEY_FIELD, key);
                    local_key_list.add(key);
                    System.out.println("Inserting values :key-- " + key + " val:" + val);
                    String filename = key;
                    FileOutputStream outputStream;

                    try {
                        outputStream = context.openFileOutput(filename, Context.MODE_PRIVATE);
                        outputStream.write(val.getBytes());
                        outputStream.close();
                    }
                    catch(Exception e){
                        System.out.println("FILE WRITE FAILED");
                    }
                    //forward to the next 2 sucessors.....

                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(succ_node)); //
                        PrintWriter out =
                                new PrintWriter(socket.getOutputStream(), true);
                        ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                        //obj_out_old.flush();
                        String req_type = "2";
                        Log.e(TAG, "Forwarding insert: " + key + ".." + val);
                        Log.e(TAG, "comp:" + my_hash.compareTo(genHash(key)) + "to:" + succ_node);
                        obj_out.writeObject(req_type);
                        obj_out.writeObject(myPort);
                        obj_out.writeObject(key);
                        obj_out.writeObject(val);
                        //socket.close();
                    }
                    catch(IOException e){
                        Log.e(TAG,"Error forwarding insert to first succ:"+succ_node+e);
                    }
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(second_succ_node)); //
                        PrintWriter out =
                                new PrintWriter(socket.getOutputStream(), true);
                        ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                        //obj_out_old.flush();
                        String req_type = "2";
                        Log.e(TAG, "Forwarding insert: " + key + ".." + val);
                        Log.e(TAG, "comp:" + my_hash.compareTo(genHash(key)) + "to:" + second_succ_node);
                        obj_out.writeObject(req_type);
                        obj_out.writeObject(myPort);
                        obj_out.writeObject(key);
                        obj_out.writeObject(val);
                        //socket.close();
                    }
                    catch(IOException e){
                        Log.e(TAG,"Error forwarding insert to second succ:"+second_succ_node+e);
                }

                }
                else if(genHash(key).compareTo(my_hash)<0 && genHash(key).compareTo(my_pred)>0){
                    //I will store it..
                    Log.e(TAG, "My Hash:" + my_hash + "..key hash:" + genHash(key) + "..comp:" + my_hash.compareTo(genHash(key)));
                    ContentValues val_insrt = new ContentValues();
                    val_insrt.put(VALUE_FIELD, val);
                    val_insrt.put(KEY_FIELD, key);
                    local_key_list.add(key);

                    System.out.println("Extracted Values:key-- " + key + " val:" + val);
                    String filename = key;
                    FileOutputStream outputStream;

                    try {
                        outputStream = context.openFileOutput(filename, Context.MODE_PRIVATE);
                        outputStream.write(val.getBytes());
                        outputStream.close();
                    }
                    catch(Exception e){
                        System.out.println("FILE WRITE FAILED");
                    }
                    //forward to the next 2 sucessors..

                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(succ_node)); //
                        PrintWriter out =
                                new PrintWriter(socket.getOutputStream(), true);
                        ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                        //obj_out_old.flush();
                        String req_type = "2";
                        Log.e(TAG, "Forwarding insert: " + key + ".." + val);
                        Log.e(TAG, "comp:" + my_hash.compareTo(genHash(key)) + "to:" + succ_node);
                        obj_out.writeObject(req_type);
                        obj_out.writeObject(myPort);
                        obj_out.writeObject(key);
                        obj_out.writeObject(val);
                        //socket.close();
                    }
                    catch(IOException e){
                        Log.e(TAG,"Error forwarding insert to first succ:"+succ_node+e);
                    }
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(second_succ_node)); //
                        PrintWriter out =
                                new PrintWriter(socket.getOutputStream(), true);
                        ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                        //obj_out_old.flush();
                        String req_type = "2";
                        Log.e(TAG, "Forwarding insert: " + key + ".." + val);
                        Log.e(TAG, "comp:" + my_hash.compareTo(genHash(key)) + "to:" + second_succ_node);
                        obj_out.writeObject(req_type);
                        obj_out.writeObject(myPort);
                        obj_out.writeObject(key);
                        obj_out.writeObject(val);
                        //socket.close();
                    }
                    catch(IOException e){
                        Log.e(TAG, "Error forwarding insert to second succ:" + second_succ_node+e);
                    }
                }
                else{
                    //pass to the correct node that handles this section...
                    // also to its next 2 successors....
                    Log.e(TAG,"passing forward to correct node...");
                    String correct_coordinator = new String();
                    String coordinator_succ = new String();
                    String coordinator_succ2 = new String();
                    String coordinator_succ_node = new String();
                    String coordinator_succ2_node = new String();
                    if(genHash(key).compareTo(min_hash)<0 || genHash(key).compareTo(max_hash)>0)
                    {
                        correct_coordinator = node_map.get(min_hash);
                    }
                    else{

                        for(String hash : node_hash_list){
                            if(hash.compareTo(min_hash)!=0){
                                if(hash.compareTo(genHash(key))>0){
                                    correct_coordinator = node_map.get(hash);
                                    break;
                                }
                            }
                        }
                    }
                    Log.e(TAG,"Correct Coordinator is :"+genHash(Integer.toString(Integer.parseInt(correct_coordinator)/2)));
                    if((node_hash_list.indexOf(genHash(Integer.toString(Integer.parseInt(correct_coordinator)/2)))+1) == node_hash_list.size())
                    {
                        coordinator_succ = node_hash_list.get(0);
                        coordinator_succ_node = node_map.get(coordinator_succ);
                    }
                    else
                    {
                        coordinator_succ = node_hash_list.get(node_hash_list.indexOf(genHash(Integer.toString(Integer.parseInt(correct_coordinator)/2)))+1);
                        coordinator_succ_node = node_map.get(coordinator_succ);
                    }

                    if((node_hash_list.indexOf(coordinator_succ)+1) == node_hash_list.size())
                    {
                        coordinator_succ2 = node_hash_list.get(0);
                        coordinator_succ2_node = node_map.get(coordinator_succ2);
                    }
                    else
                    {
                        coordinator_succ2 = node_hash_list.get(node_hash_list.indexOf(coordinator_succ)+1);
                        coordinator_succ2_node = node_map.get(coordinator_succ2);
                    }



                    Log.e(TAG,"correct cordinator for "+ genHash(key)+" is: "+correct_coordinator+"..."+genHash(correct_coordinator));
                    Log.e(TAG,"Inserting into....."+correct_coordinator+"...hash::"+genHash(Integer.toString(Integer.parseInt(correct_coordinator) / 2)));
                    Log.e(TAG,"whose first successor is......... :"+ coordinator_succ);
                    Log.e(TAG,"and second  successor is......... :"+ coordinator_succ2);



                    try {

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(correct_coordinator)); //


                        ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                        //obj_out_old.flush();
                        String req_type = "1";
                        Log.e(TAG, "Passing insert request to correct node: " + key + ".." + val);
                        Log.e(TAG, "comp:" + my_hash.compareTo(genHash(key)) + "to:" + correct_coordinator);
                        obj_out.writeObject(req_type);
                        obj_out.writeObject(key);
                        obj_out.writeObject(val);
                        //obj_out.close();
                        //socket.close();
                        }
                        catch(IOException e){
                        Log.e(TAG,"Error forwarding insert--coordinator:"+correct_coordinator+e);
                    }

                        try{
                        //send to succ1---
                        Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(coordinator_succ_node));

                        ObjectOutputStream obj_out1 = new ObjectOutputStream(socket1.getOutputStream());
                        //obj_out_old.flush();
                        String req_type = "2";
                        Log.e(TAG, "Passing insert request to coordinator succ-1: " + key + ".." + val);
                        obj_out1.writeObject(req_type);
                        obj_out1.writeObject(correct_coordinator);
                        obj_out1.writeObject(key);
                        obj_out1.writeObject(val);
                            //obj_out1.close();
                        //socket1.close();
                    }
                        //send to succ2---
                        catch(IOException e){
                            Log.e(TAG,"Error forwarding insert--succ1:"+coordinator_succ_node+e);
                        }
                        try{
                        Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(coordinator_succ2_node)); //
                        PrintWriter out2 =
                                new PrintWriter(socket2.getOutputStream(), true);
                        ObjectOutputStream obj_out2 = new ObjectOutputStream(socket2.getOutputStream());
                        //obj_out_old.flush();
                        String req_type = "2";
                        Log.e(TAG, "Passing insert request to coordinator succ-2: " + key + ".." + val);

                            obj_out2.writeObject(req_type);
                            obj_out2.writeObject(correct_coordinator);
                            obj_out2.writeObject(key);
                        obj_out2.writeObject(val);
                            //obj_out2.close();
                        //socket2.close();
                    }
                    catch(IOException e){
                        Log.e(TAG,"Error forwarding insert--succ2:"+coordinator_succ2_node+e);
                    }

                    //new forward_insert_request().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, succ_node, key, val); // edit this...
                    // change code for server to handle insert requests..
                }
            }
            catch (NoSuchAlgorithmException e){
                Log.e(TAG, "Cannot hash insert key");
            }


            return null;

    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        //redundant ....but verifying
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.e(TAG, "My PORT is: " + myPort);
        try {
            my_hash = genHash(Integer.toString(Integer.parseInt(myPort) / 2));
            System.out.println("My Hash value is:" + my_hash);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "my hash gen failed" + e);
        }
        my_pred = my_hash;
        my_succ =  my_hash;
        update_pred_succ();
        neigh_list = new ArrayList<String>(Arrays.asList(succ_node,pred_node,second_pred_node));
        //statically update predd and succ information... successfull
        //see if succ exists and has any keys inserted into it...
        //if keys exists..
        //Client Task...
        if(local_key_list.size() ==0 && pred_key_list.size() == 0 && pred2_key_list.size()==0
                && succ_key_list.size()==0){
            Log.e(TAG,"No keys exists on create....");

        }
        for (String node : neigh_list) {
            new RejoinTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,node);
        }


        // set server for incoming requests..
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,serverSocket);
            //CORRECT ???
            //ServerSocket  requestSocket = new ServerSocket(SERVER_PORT);
            //new RequestServerTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestSocket);
        }
        catch(IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }



        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key","value"});
        Context context = getContext();
        System.out.println("Key recvd is: " + selection);
        try {
            if (selection.compareTo("@") == 0) {
                //check if stand alone case
                for (String key : local_key_list) {
                    try {
                        // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                        String filename = key;
                        FileInputStream inputStream;
                        inputStream = context.openFileInput(filename);
                        int avail = inputStream.available();
                        byte[] value = new byte[avail];
                        inputStream.read(value);
                        inputStream.close();
                        String read_val = new String(value, "UTF-8");
                        //reader.close();
                        System.out.println("The value read is:" + read_val);
                        //Can correctly read from file: Check how to send it as cursor
                        Object[] ret_cursor = {filename, read_val};
                        matrixCursor.addRow(ret_cursor);
                        Log.e(TAG,"Total Keys stored in this node:"+local_key_list.size()+" "+succ_key_list.size()+" "+
                                succ2_key_list.size()+" "+pred_key_list.size()+" "+pred2_key_list.size());
                    } catch (Exception e) {
                        System.out.println("FILE OPEN FAILED");
                    }

                    Log.v("query", selection);
                }
                for (String key : pred_key_list) {
                    try {
                        // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                        String filename = key;
                        FileInputStream inputStream;
                        inputStream = context.openFileInput(filename);
                        int avail = inputStream.available();
                        byte[] value = new byte[avail];
                        inputStream.read(value);
                        inputStream.close();
                        String read_val = new String(value, "UTF-8");
                        //reader.close();
                        System.out.println("The value read is:" + read_val);
                        //Can correctly read from file: Check how to send it as cursor
                        Object[] ret_cursor = {filename, read_val};
                        matrixCursor.addRow(ret_cursor);
                        Log.e(TAG,"Total Keys stored in this node:"+local_key_list.size()+" "+succ_key_list.size()+" "+
                                succ2_key_list.size()+" "+pred_key_list.size()+" "+pred2_key_list.size());
                    } catch (Exception e) {
                        System.out.println("FILE OPEN FAILED");
                    }

                    Log.v("query", selection);
                }
                for (String key : pred2_key_list) {
                    try {
                        // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                        String filename = key;
                        FileInputStream inputStream;
                        inputStream = context.openFileInput(filename);
                        int avail = inputStream.available();
                        byte[] value = new byte[avail];
                        inputStream.read(value);
                        inputStream.close();
                        String read_val = new String(value, "UTF-8");
                        //reader.close();
                        System.out.println("The value read is:" + read_val);
                        //Can correctly read from file: Check how to send it as cursor
                        Object[] ret_cursor = {filename, read_val};
                        matrixCursor.addRow(ret_cursor);
                        Log.e(TAG, "Total Keys stored in this node:" + local_key_list.size()+" "+succ_key_list.size()+" "+
                                succ2_key_list.size()+" "+pred_key_list.size()+" "+pred2_key_list.size());
                    } catch (Exception e) {
                        System.out.println("FILE OPEN FAILED");
                    }

                    Log.v("query", selection);
                }
            }
            else if(selection.compareTo("*") == 0){
                //global all query...
                Map<String,String> global_vals = new HashMap<String, String>();
                for(String key : local_key_list){
                    //get local stored values
                    try {
                        // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                        String filename = key;
                        FileInputStream inputStream;
                        inputStream = context.openFileInput(filename);
                        int avail = inputStream.available();
                        byte[] value = new byte[avail];
                        inputStream.read(value);
                        inputStream.close();
                        String read_val = new String(value, "UTF-8");
                        //reader.close();
                        System.out.println("The value read is:" + read_val);
                        global_vals.put(key,read_val);
                        //Can correctly read from file: Check how to send it as cursor

                    } catch (Exception e) {
                        System.out.println("FILE OPEN FAILED");
                    }

                }
                Log.e(TAG, "My own rows: " + global_vals.size());
                for(String node : node_list){
                    if(node.compareTo(myPort)!=0) {
                        try {

                            Log.e(TAG, "propagating query * global to:" + node);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(node)); //
                            ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                            //obj_out_old.flush();
                            String req_type = "-1";
                            obj_out.writeObject(req_type);
                            ObjectInputStream obj_inp = new ObjectInputStream(socket.getInputStream());
                            Object returned_keys = obj_inp.readObject();
                            Map<String,String> retKey = (HashMap<String,String>) returned_keys;
                            global_vals.putAll(retKey);
                        }
                        catch(IOException e){
                            Log.e(TAG,"Error sending global * query"+e);
                        }
                        catch(ClassNotFoundException e){
                            Log.e(TAG,"Error reading global * query"+e);
                        }
                    }
                }

                for(String key : global_vals.keySet()){
                    try {
                        // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                        Log.e(TAG,"Total rows: "+global_vals.size());
                        Object[] ret_cursor = {key,global_vals.get(key)};
                        matrixCursor.addRow(ret_cursor);
                    } catch (Exception e) {
                        System.out.println("FILE OPEN FAILED");
                    }

                    Log.v("query", key);

                }

            }
            else if ((my_hash.compareTo(genHash(selection)) > 0 && my_pred.compareTo(genHash(selection)) < 0)
                    || (my_hash.compareTo(min_hash) == 0 && max_hash.compareTo(genHash(selection)) < 0)
                    || (my_hash.compareTo(min_hash) == 0 && my_hash.compareTo(genHash(selection)) > 0))
            {
                // key is in my LFS
                try {
                    // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                    String filename = selection;
                    FileInputStream inputStream;
                    inputStream = context.openFileInput(filename);
                    int avail = inputStream.available();
                    byte[] value = new byte[avail];
                    inputStream.read(value);
                    inputStream.close();
                    String read_val = new String(value, "UTF-8");
                    //reader.close();
                    System.out.println("The value read is:" + read_val);
                    //Can correctly read from file: Check how to send it as cursor
                    Object[] ret_cursor = {filename, read_val};
                    matrixCursor.addRow(ret_cursor);
                } catch (Exception e) {
                    System.out.println("FILE OPEN FAILED");
                }

                Log.v("query", selection);


            } else {

                //pass to correct coordinator
                String correct_coordinator = new String();
                String coordinator_succ = new String();
                String coordinator_succ_node =  new String();
                String coordinator_succ2 = new String();
                String coordinator_succ2_node = new String();
                Log.e(TAG,"passing query forward...");
                if(genHash(selection).compareTo(min_hash)<0 || genHash(selection).compareTo(max_hash)>0)
                {
                    correct_coordinator = node_map.get(min_hash);
                }
                else{

                    for(String hash : node_hash_list){
                        if(hash.compareTo(min_hash)!=0){
                            if(hash.compareTo(genHash(selection))>0){
                                correct_coordinator = node_map.get(hash);
                                break;
                            }
                        }
                    }
                }

                Log.e(TAG,"correct cordinator for "+ genHash(selection)+" is: "+correct_coordinator +"..."+genHash(correct_coordinator));
                if((node_hash_list.indexOf(genHash(Integer.toString(Integer.parseInt(correct_coordinator)/2)))+1) == node_hash_list.size())
                {
                    coordinator_succ = node_hash_list.get(0);
                    coordinator_succ_node = node_map.get(coordinator_succ);
                }
                else
                {
                    coordinator_succ = node_hash_list.get(node_hash_list.indexOf(genHash(Integer.toString(Integer.parseInt(correct_coordinator)/2)))+1);
                    coordinator_succ_node = node_map.get(coordinator_succ);
                }

                if((node_hash_list.indexOf(coordinator_succ)+1) == node_hash_list.size())
                {
                    coordinator_succ2 = node_hash_list.get(0);
                    coordinator_succ2_node = node_map.get(coordinator_succ2);
                }
                else
                {
                    coordinator_succ2 = node_hash_list.get(node_hash_list.indexOf(coordinator_succ)+1);
                    coordinator_succ2_node = node_map.get(coordinator_succ2);
                }



                Log.e(TAG,"correct cordinator for "+ genHash(selection)+" is: "+correct_coordinator+"..."+genHash(correct_coordinator));

                try {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(correct_coordinator)); //
                    ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                    //obj_out_old.flush();
                    String req_type = "3";
                    Log.e(TAG, "Passing query request: " + selection);
                    Log.e(TAG, "comp:" + my_hash.compareTo(genHash(selection)) + "to:" + correct_coordinator);
                    obj_out.writeObject(req_type);
                    obj_out.writeObject(selection);
                    //socket.close();\
                    ObjectInputStream obj_inp = new ObjectInputStream(socket.getInputStream());
                    Object returnkey = obj_inp.readObject();
                    String retKey = (String) returnkey;
                    Object returnvalue = obj_inp.readObject();
                    String retVal = (String) returnvalue;
                    Object[] ret_cursor = {retKey, retVal};
                    matrixCursor.addRow(ret_cursor);
                }
                catch(IOException e){
                    Log.e(TAG,"Error forwarding query:"+correct_coordinator+e);
                    Log.e(TAG,"Sedning Query to Coordinators Successor.. "+coordinator_succ_node);
                    try {

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(coordinator_succ_node)); //
                        ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                        //obj_out_old.flush();
                        String req_type = "5";
                        Log.e(TAG, "Passing query request: " + selection);
                        Log.e(TAG, "comp:" + my_hash.compareTo(genHash(selection)) + "to:" + coordinator_succ_node);
                        obj_out.writeObject(req_type);
                        obj_out.writeObject(selection);
                        //socket.close();\
                        ObjectInputStream obj_inp = new ObjectInputStream(socket.getInputStream());
                        Object returnkey = obj_inp.readObject();
                        String retKey = (String) returnkey;
                        Object returnvalue = obj_inp.readObject();
                        String retVal = (String) returnvalue;
                        Object[] ret_cursor = {retKey, retVal};
                        matrixCursor.addRow(ret_cursor);
                    }
                    catch(IOException e2)
                    {
                        Log.e(TAG,"Error sending to coordinators sucessor"+e2);
                        try {

                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(coordinator_succ2_node)); //
                            ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                            //obj_out_old.flush();
                            String req_type = "5";
                            Log.e(TAG, "Passing query request: " + selection);
                            Log.e(TAG, "comp:" + my_hash.compareTo(genHash(selection)) + "to:" + coordinator_succ2_node);
                            obj_out.writeObject(req_type);
                            obj_out.writeObject(selection);
                            //socket.close();\
                            ObjectInputStream obj_inp = new ObjectInputStream(socket.getInputStream());
                            Object returnkey = obj_inp.readObject();
                            String retKey = (String) returnkey;
                            Object returnvalue = obj_inp.readObject();
                            String retVal = (String) returnvalue;
                            Object[] ret_cursor = {retKey, retVal};
                            matrixCursor.addRow(ret_cursor);
                        }
                        catch(IOException e3)
                        {
                            Log.e(TAG,"Error sending to coordinators second sucessor"+e3);
                        }
                        catch(ClassNotFoundException e4)
                        {
                            Log.e(TAG,"Error sending to coordinators second sucessor"+e4);
                        }
                    }
                    catch(ClassNotFoundException e3){
                        Log.e(TAG,"error reading result of passed query from coordinator succ"+e);
                    }

                }
                catch (ClassNotFoundException e){
                    Log.e(TAG,"error reading result of passed query"+e);
                }

            }

        }
        catch (NoSuchAlgorithmException e){
            Log.e(TAG,"Error gen hash for selection in query"+e);
        }
        return matrixCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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

    //method for update predd-succ for a node....
    private void update_pred_succ() {
        try {
            min_hash = genHash(Integer.toString(Integer.parseInt(node_list.get(0)) / 2));
            max_hash = genHash(Integer.toString(Integer.parseInt(node_list.get(0)) / 2));
            max_hash_node = new String(); //
            min_hash_node = new String(); //
            for (String node : node_list) {
                String node_hash = genHash(Integer.toString(Integer.parseInt(node) / 2));
                if (min_hash.compareTo(node_hash) > 0) {
                    min_hash = node_hash;
                    min_hash_node = node;
                } else if (max_hash.compareTo(node_hash) < 0) {
                    max_hash = node_hash;
                    max_hash_node = node;
                }
                Log.e(TAG, "Min:" + min_hash + "max:" + max_hash);
                node_hash_list.add(node_hash);
                node_map.put(node_hash,node);
            }
            Collections.sort(node_hash_list);

            for (String node : node_list) {
                try {
                    if (node.compareTo(myPort) != 0) {
                        System.out.println("Node is:" + node);
                        String node_hash = genHash(Integer.toString(Integer.parseInt(node) / 2));
                        if (node_list.size() == 2 && (Integer.parseInt(myPort) / 2) != 5554) {
                            my_pred = node_hash;
                            my_succ = node_hash;
                            pred_node = node;
                            succ_node = node;
                            Log.e(TAG, "My new succ+pred:" + node_hash);
                        } //first case
                        else if (my_hash.compareTo(node_hash) < 0 && my_hash.compareTo(my_succ) == 0) {
                            my_succ = node_hash;
                            succ_node = node;
                        } else if (my_hash.compareTo(node_hash) > 0 && my_hash.compareTo(my_pred) == 0) {
                            my_pred = node_hash;
                            pred_node = node;
                        } else if (my_succ.compareTo(min_hash) == 0 && my_hash.compareTo(node_hash) < 0) {
                            my_succ = node_hash;
                            succ_node = node;
                        } else if (my_pred.compareTo(max_hash) == 0 && min_hash.compareTo(node_hash) == 0) {
                            my_pred = node_hash;
                            pred_node = node;
                        } else if (my_succ.compareTo(node_hash) > 0 && my_hash.compareTo(node_hash) < 0) {
                            my_succ = node_hash;
                            succ_node = node;
                        } else if (my_pred.compareTo(node_hash) < 0 && my_hash.compareTo(node_hash) > 0) {
                            my_pred = node_hash;
                            pred_node = node;
                        }
                    }
                } catch (NoSuchAlgorithmException e) {
                    Log.e(TAG, "Failed to gen hash:" + e);
                }
            }
            if (node_list.size() != 2) {
                if (my_hash.compareTo(max_hash) == 0) {
                    my_succ = min_hash;
                    succ_node = min_hash_node;
                } else if (my_hash.compareTo(min_hash) == 0) {
                    my_pred = max_hash;
                    pred_node = max_hash_node;
                }
            }

            if((node_hash_list.indexOf(my_succ)+1) == node_hash_list.size())
            {
                second_succ = node_hash_list.get(0);
                second_succ_node = node_map.get(second_succ);
            }
            else
            {
                second_succ = node_hash_list.get(node_hash_list.indexOf(my_succ)+1);
                second_succ_node = node_map.get(second_succ);
            }

            if(node_hash_list.indexOf(my_pred) == 0)
            {
                second_pred = node_hash_list.get(node_hash_list.size()-1);
                second_pred_node = node_map.get(second_pred);

            }
            else
            {
                second_pred = node_hash_list.get((node_hash_list.indexOf(my_pred)-1));
                second_pred_node = node_map.get(second_pred);
            }



            Log.e(TAG, "my updated succ is:" + my_succ);
            Log.e(TAG, "my updated succ is:" + succ_node);
            Log.e(TAG, "my updated second succ is:" + second_succ);
            Log.e(TAG, "my updated second succ is:" + second_succ_node);
            Log.e(TAG, "my updated second pred is:" + second_pred);
            Log.e(TAG, "my updated second pred is:" + second_pred_node);
        }   catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "node list error1:" + e);
        }

    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets){
            ServerSocket serverSocket = sockets[0];
            System.out.println("Starting Master Server");
            try {
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    //serverSocket.setSoTimeout(2000);
                    //PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true); //trying 2-way
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    ObjectInputStream obj_inp = new ObjectInputStream(clientSocket.getInputStream());
                    Object req_obj = obj_inp.readObject();
                    String req_type = (String) req_obj;
                    if (Integer.parseInt(req_type) == 1) { // Insert request..
                        Object key_obj = obj_inp.readObject();
                        String key = (String) key_obj;
                        Object val_obj = obj_inp.readObject();
                        String val = (String) val_obj;
                        //---- Inserting k-v to my node..
                        System.out.println("Inserting values :key-- " + key + " val:" + val);
                        String filename = key;
                        FileOutputStream outputStream;
                        local_key_list.add(key);
                        try {
                            outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                            outputStream.write(val.getBytes());
                            outputStream.close();
                        }
                        catch(Exception e){
                            System.out.println("FILE WRITE FAILED for recieved insert req to my node.. "+e);
                        }

                        /*
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
                        uriBuilder.scheme("content");
                        Uri mUri = uriBuilder.build();
                        ContentValues mContentValues = new ContentValues();
                        ContentResolver mContentResolver = getContext().getContentResolver();

                        mContentValues.put("key", key);
                        mContentValues.put("value", val);
                        mContentResolver.insert(mUri, mContentValues);
                        */

                    }
                    else if(Integer.parseInt(req_type) == 2) { //replicated key insert request...
                        Object port_obj = obj_inp.readObject();
                        String port = (String) port_obj;
                        Object key_obj = obj_inp.readObject();
                        String key = (String) key_obj;
                        Object val_obj = obj_inp.readObject();
                        String val = (String) val_obj;
                        //store into local FS
                        if(port.compareTo(pred_node)==0) {

                            pred_key_list.add(key);

                        }
                        else if(port.compareTo(second_pred_node)==0)
                        {
                            pred2_key_list.add(key);
                        }

                        System.out.println("Inserting replicated values :key-- " + key + " val:" + val+ "of: "+port);
                        String filename = key;
                        FileOutputStream outputStream;

                        try {
                            outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                            outputStream.write(val.getBytes());
                            outputStream.close();
                        }
                        catch(Exception e){
                            System.out.println("FILE WRITE FAILED for replicated key"+e);
                        }

                    }
                    else if(Integer.parseInt(req_type) == 3){ //Query Request
                        Object key_obj = obj_inp.readObject();
                        String selection = (String) key_obj;
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
                        uriBuilder.scheme("content");
                        Uri mUri = uriBuilder.build();
                        ContentValues mContentValues = new ContentValues();
                        ContentResolver mContentResolver = getContext().getContentResolver();
                        Cursor resultCursor = mContentResolver.query(mUri, null,
                                selection, null, null);
                        int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                        int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                        resultCursor.moveToFirst();
                        String returnKey = resultCursor.getString(keyIndex);
                        String returnValue = resultCursor.getString(valueIndex);
                        resultCursor.close();
                        ObjectOutputStream obj_out = new ObjectOutputStream(clientSocket.getOutputStream());
                        obj_out.writeObject(returnKey);
                        obj_out.writeObject(returnValue);
                    }
                    else if(Integer.parseInt(req_type) == 4){ //Rejoin Node Request...
                        Object port_obj = obj_inp.readObject();
                        String port = (String) port_obj;
                        Log.e(TAG,"Rejoin Node Request..: "+port_obj);
                        if(local_key_list.size() ==0 && pred_key_list.size() == 0 && pred2_key_list.size()==0
                                && succ_key_list.size()==0){ //no keys exist ..
                            ObjectOutputStream obj_out = new ObjectOutputStream(clientSocket.getOutputStream());
                            obj_out.writeObject("0");
                        }
                        else {
                            ObjectOutputStream obj_out = new ObjectOutputStream(clientSocket.getOutputStream());
                            obj_out.writeObject("1");
                            if(port.compareTo(succ_node)==0)
                            {
                                obj_out.writeObject(myPort); //node to which these key,vals belong..
                                Map<String, String> key_val_map = new HashMap<String, String>();
                                Log.e(TAG,"Sending back my own keys: "+local_key_list.size());
                                for(String key : local_key_list){
                                    //put key value in a single list.. send list to rejoining node..
                                    String filename = key;
                                    FileInputStream inputStream;
                                    inputStream = getContext().openFileInput(filename);
                                    int avail = inputStream.available();
                                    byte[] value = new byte[avail];
                                    inputStream.read(value);
                                    inputStream.close();
                                    String read_val = new String(value, "UTF-8");
                                    key_val_map.put(key,read_val);
                                }
                                obj_out.writeObject(key_val_map);

                            }else if(port.compareTo(second_succ_node)==0)
                            {
                                obj_out.writeObject("special"); //node to which these key,vals belong..
                                //first my keys... -- this is only for 4 AVD CASE : CHANGE IT FOR % !! IMP
                                Map<String, String> key_val_map = new HashMap<String, String>();
                                Log.e(TAG, "Sending back my keys : " + local_key_list.size());
                                for(String key : local_key_list){
                                    //put key value in a single list.. send list to rejoining node..
                                    String filename = key;
                                    FileInputStream inputStream;
                                    inputStream = getContext().openFileInput(filename);
                                    int avail = inputStream.available();
                                    byte[] value = new byte[avail];
                                    inputStream.read(value);
                                    inputStream.close();
                                    String read_val = new String(value, "UTF-8");
                                    key_val_map.put(key,read_val);
                                }
                                obj_out.writeObject(key_val_map);

                            }
                            else if(port.compareTo(pred_node)==0) {
                                obj_out.writeObject(pred_node); //node to which these key,vals belong..
                                Map<String, String> key_val_map = new HashMap<String, String>();
                                Log.e(TAG, "Sending back nodes keys: " + pred_key_list.size());
                                for(String key : pred_key_list){
                                    //put key value in a single list.. send list to rejoining node..
                                    String filename = key;
                                    FileInputStream inputStream;
                                    inputStream = getContext().openFileInput(filename);
                                    int avail = inputStream.available();
                                    byte[] value = new byte[avail];
                                    inputStream.read(value);
                                    inputStream.close();
                                    String read_val = new String(value, "UTF-8");
                                    key_val_map.put(key,read_val);
                                }
                                obj_out.writeObject(key_val_map);
                            }
                            /* --- succ2 and pred 2 combined...
                            else if(port.compareTo(second_pred_node)==0)
                            {
                                obj_out.writeObject("special"); //node to which these key,vals belong..
                                obj_out.writeObject(myPort);
                                Map<String, String> key_val_map = new HashMap<String, String>();
                                for(String key : succ_key_list){
                                    //put key value in a single list.. send list to rejoining node..
                                    String filename = key;
                                    FileInputStream inputStream;
                                    inputStream = getContext().openFileInput(filename);
                                    int avail = inputStream.available();
                                    byte[] value = new byte[avail];
                                    inputStream.read(value);
                                    inputStream.close();
                                    String read_val = new String(value, "UTF-8");
                                    key_val_map.put(key,read_val);
                                }
                                obj_out.writeObject(key_val_map);
                            } */
                        }

                    }
                    else if(Integer.parseInt(req_type) == 5){ // query for a replicated key..
                        try {
                            // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                            Object selection_obj = obj_inp.readObject();
                            String selection = (String) selection_obj;
                            String filename = selection;
                            FileInputStream inputStream;
                            inputStream = getContext().openFileInput(filename);
                            int avail = inputStream.available();
                            byte[] value = new byte[avail];
                            inputStream.read(value);
                            inputStream.close();
                            String read_val = new String(value, "UTF-8");
                            //reader.close();
                            System.out.println("The value read is:" + read_val);
                            //Can correctly read from file: Check how to send it as cursor
                            ObjectOutputStream obj_out = new ObjectOutputStream(clientSocket.getOutputStream());
                            obj_out.writeObject(selection);
                            obj_out.writeObject(read_val);
                        } catch (Exception e) {
                            System.out.println("FILE OPEN FAILED");
                        }


                    }
                    else if (Integer.parseInt(req_type) == 6) { //forwarded delete
                        Object selection_obj = obj_inp.readObject();
                        String selection = (String) selection_obj;
                        String filename = selection;
                        try {
                            // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                            getContext().deleteFile(selection);
                        } catch (Exception e) {
                            System.out.println("FILE Delete FAILED for recieved request... ");
                        }
                        local_key_list.remove(selection);
                        pred_key_list.remove(selection);
                        pred2_key_list.remove(selection);
                    }
                    else if (Integer.parseInt(req_type) == -1) { //A global * query
                        Map<String, String> my_local_vals = new HashMap<String, String>();
                        for (String key : local_key_list) {
                            //get local stored values
                            try {
                                // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                                String filename = key;
                                FileInputStream inputStream;
                                inputStream = getContext().openFileInput(filename);
                                int avail = inputStream.available();
                                byte[] value = new byte[avail];
                                inputStream.read(value);
                                inputStream.close();
                                String read_val = new String(value, "UTF-8");
                                //reader.close();
                                System.out.println("The value read is:" + read_val);
                                my_local_vals.put(key, read_val);
                                //Can correctly read from file: Check how to send it as cursor
                            } catch (Exception e) {
                                System.out.println("FILE OPEN FAILED");
                            }

                        }
                        for (String key : pred_key_list) {
                            //get local stored values
                            try {
                                // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                                String filename = key;
                                FileInputStream inputStream;
                                inputStream = getContext().openFileInput(filename);
                                int avail = inputStream.available();
                                byte[] value = new byte[avail];
                                inputStream.read(value);
                                inputStream.close();
                                String read_val = new String(value, "UTF-8");
                                //reader.close();
                                System.out.println("The value read is:" + read_val);
                                my_local_vals.put(key, read_val);
                                //Can correctly read from file: Check how to send it as cursor
                            } catch (Exception e) {
                                System.out.println("FILE OPEN FAILED");
                            }

                        }
                        for (String key : pred2_key_list) {
                            //get local stored values
                            try {
                                // InputStream inputStream = new BufferedInputStream(new FileInputStream(filename),1024);
                                String filename = key;
                                FileInputStream inputStream;
                                inputStream = getContext().openFileInput(filename);
                                int avail = inputStream.available();
                                byte[] value = new byte[avail];
                                inputStream.read(value);
                                inputStream.close();
                                String read_val = new String(value, "UTF-8");
                                //reader.close();
                                System.out.println("The value read is:" + read_val);
                                my_local_vals.put(key, read_val);
                                //Can correctly read from file: Check how to send it as cursor
                            } catch (Exception e) {
                                System.out.println("FILE OPEN FAILED");
                            }

                        }
                        Log.e(TAG, "Returning rows: " + my_local_vals.size());
                        ObjectOutputStream obj_out = new ObjectOutputStream(clientSocket.getOutputStream());
                        obj_out.writeObject(my_local_vals);
                    }
                }
            }
            catch(IOException e){

                Log.e(TAG, "Cant create Master Server"+e);
            }
            catch(ClassNotFoundException e){
                Log.e(TAG,"error reading object from object stream"+e);
            }
            return null;
        }
    }

    private class RejoinTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[0])); //
                PrintWriter out =
                        new PrintWriter(socket.getOutputStream(), true);
                ObjectOutputStream obj_out = new ObjectOutputStream(socket.getOutputStream());
                //obj_out_old.flush();
                String req_type = "4"; //Replicas Exist??
                Log.e(TAG, "Rejoining enquiry to: "+msgs[0]);
                obj_out.writeObject(req_type);
                obj_out.writeObject(myPort);
                //socket.close();
                ObjectInputStream obj_inp = new ObjectInputStream(socket.getInputStream());
                Object response = obj_inp.readObject();
                String ret =   (String) response;
                if(Integer.parseInt(ret) == 0){
                    Log.e(TAG,"New create..");
                }
                else if (Integer.parseInt(ret) == 1){
                    Log.e(TAG,"Rejoin After Failure..");
                    Object resp_obj = obj_inp.readObject();
                    String resp =   (String) resp_obj;
                    Log.e(TAG,"response is : "+resp);
                    if(resp.compareTo("special")==0){
                        Object key_val_list_obj = obj_inp.readObject();
                        Map<String, String> key_val = (HashMap) key_val_list_obj;
                        for(String key : key_val.keySet()){
                            String val = key_val.get(key);
                            //put in my list..
                            String filename = key;
                            FileOutputStream outputStream;
                            Log.e(TAG,"Inserting regained k-v from second predd: "+key+"..."+val);
                            try {
                                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                                outputStream.write(val.getBytes());
                                outputStream.close();
                            }
                            catch(Exception e){
                                System.out.println("FILE WRITE FAILED on rejoin.. ");
                            }

                            pred2_key_list.add(key);
                        }
                    }
                    else {
                        String port = resp;
                        Object key_val_list_obj = obj_inp.readObject();
                        Map<String, String> key_val = (HashMap) key_val_list_obj;
                        for(String key : key_val.keySet()){
                            String val = key_val.get(key);
                            //put in my list..
                            String filename = key;
                            FileOutputStream outputStream;

                            try {
                                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                                outputStream.write(val.getBytes());
                                outputStream.close();
                            }
                            catch(Exception e){
                                System.out.println("FILE WRITE FAILED on rejoin.. ");
                            }
                            if(port.compareTo(myPort)==0){
                            local_key_list.add(key);
                                Log.e(TAG, "Inserting regained k-v from succ: " + key + "..." + val);
                            }
                            else if(port.compareTo(pred_node)==0) {
                                pred_key_list.add(key);
                                Log.e(TAG, "Inserting regained k-v from predd: " + key + "..." + val);
                            }
                        }
                    }
                }

                Log.e(TAG,"After Rejoin:-- MY LIST:"+local_key_list.size()+"..pred list:"+pred_key_list.size()
                        +"...pred2 list:"+pred2_key_list.size());
            }
            catch(IOException e){
                Log.e(TAG, "No" +msgs[0]+" Exists yet... " + e);
            }
            catch(ClassNotFoundException e){
                Log.e(TAG, "Error reading on rejoin.. " + e);
            }
            return null;
        }
    }

}