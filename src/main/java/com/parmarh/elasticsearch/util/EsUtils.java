package com.parmarh.elasticsearch.util;

import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

public class EsUtils {

    private static Logger logger = LoggerFactory.getLogger(EsUtils.class);

    private static int numShards = 18;

    public static void main(String[] args) throws IOException
    {
        String test = "ALL CATEGORIES0.txt.gz";
        String result = test.replaceAll("\\d.txt.gz", "" );
        System.out.println( result );
    }

    public static int hash(String value, Integer numShards ) {
        long hash = 5381;
        for (int i = 0; i < value.length(); i++) {
            hash = ((hash << 5) + hash) + value.charAt(i);
        }

        return Math.abs((int) hash % numShards);
    }

    public static ArrayList<String> getRoutes( int numShards )
    {
        ArrayList<String> possibleRoutes = newArrayList();
        ArrayList<String> usedHash = newArrayList();
        Integer x = 0;
        while(possibleRoutes.size() < numShards)
        {
            Integer hash = EsUtils.hash(x.toString(), numShards );
            if ( !usedHash.contains( hash.toString() ) )
            {
                possibleRoutes.add(x.toString());
                usedHash.add( hash.toString() );
            }
            x++;
        }
        return possibleRoutes;
    }

    public static HashMap<String,Integer> getRouteMap( int numShards )
    {
        ArrayList<String> possibleRoutes = getRoutes( numShards );
        HashMap<String, Integer> routeMap = newHashMap();
        for ( int i = 0; i < numShards; i++ )
            routeMap.put( possibleRoutes.get( i ), new Integer( i ) );
        return routeMap;
    }



    public static void deleteIndex( String esEndPoint, String indexName ) throws IOException
    {
        DefaultHttpClient client  = new DefaultHttpClient();
        HttpDelete delete = new HttpDelete( esEndPoint + "/" + indexName );

        HttpResponse response = client.execute( delete );

        System.out.println( IOUtils.toString( response.getEntity().getContent(), "UTF-8" ) );
        logger.info( indexName + ": " + String.valueOf( response.getStatusLine().getStatusCode() ) );
    }

    public static void restoreSnapshot( String esEndpoint, String snapshotName ) throws IOException
    {
        logger.info( "Restore Snapshot" );

        DefaultHttpClient client = new DefaultHttpClient();

        HttpPost post = new HttpPost( esEndpoint + "/_snapshot/" + snapshotName + "/snapshot/_restore" );
        HttpResponse response = client.execute( post );

        System.out.println( IOUtils.toString( response.getEntity().getContent(), "UTF-8" ) );
        logger.info( snapshotName + ": " + String.valueOf( response.getStatusLine().getStatusCode() ) );
    }

    public static void aliasIndex( String esEndPoint, String indexName, String aliasName ) throws IOException
    {
        esEndPoint = esEndPoint + "/_alias";
        logger.info( "Alias Index" );
        Gson gson = new Gson();
        DefaultHttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost( esEndPoint + "/_alias" );
        HashMap<String, Object> body = newHashMap();

        HashMap<String,String> addAction = newHashMap();
        addAction.put( "index", indexName );
        addAction.put( "alias", aliasName );
        HashMap<String, Object> action = newHashMap();
        action.put( "add", addAction );
        ArrayList<Object> actions = newArrayList();
        actions.add( action );
        body.put( "actions", actions );

        post.setEntity( new StringEntity( gson.toJson( body ) ) );
        System.out.println( gson.toJson( body ) );

        HttpResponse response = client.execute( post );
        System.out.println( IOUtils.toString( response.getEntity().getContent(), "UTF-8" ) );
        logger.info( "Alias: " + String.valueOf( response.getStatusLine().getStatusCode() ) );
    }

    public static String postQuery( String esEndPoint, String indexName, String query ) throws IOException
    {
        esEndPoint = esEndPoint + "/" + indexName + "/_search";
        logger.info( "Search Query" );

        DefaultHttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost( esEndPoint );
        post.setEntity( new StringEntity( query ) );

        HttpResponse response = client.execute( post );
        return IOUtils.toString( response.getEntity().getContent(), "UTF-8" );
    }

    public static void indexDocument( String esEndPoint, String indexName, String id, String jsonDocument ) throws IOException
    {
        esEndPoint = esEndPoint + "/" + indexName + "/" + id;
        DefaultHttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost( esEndPoint );
        post.setEntity( new StringEntity( jsonDocument ) );

        HttpResponse response = client.execute( post );
        System.out.println( IOUtils.toString( response.getEntity().getContent(), "UTF-8" ) );
    }

    public static void enableCache( String esEndPoint, String indexName ) throws IOException
    {
        esEndPoint = esEndPoint + "/" + indexName + "/_settings";
        DefaultHttpClient client = new DefaultHttpClient();
        HttpPut put = new HttpPut( esEndPoint );
        put.setEntity( new StringEntity( "{ \"index.requests.cache.enable\": true }" ) );

        HttpResponse response = client.execute( put );
        System.out.println( IOUtils.toString( response.getEntity().getContent(), "UTF-8" ) );
    }
}
