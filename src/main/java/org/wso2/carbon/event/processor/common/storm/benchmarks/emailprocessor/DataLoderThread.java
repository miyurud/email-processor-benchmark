/**
 * 
 */
package org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor;

import com.uebercomputing.mailrecord.MailRecord;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Miyuru Dayarathna
 *
 */
public class DataLoderThread extends Thread {
	private String filePath;
	private LinkedBlockingQueue<Object> eventBufferList;
	private BufferedReader br;
	private int count;
	private String schemaPath = "";
	
	public DataLoderThread(String filePath, LinkedBlockingQueue<Object> eventBuffer){
        super("Data Loader");
        this.filePath = filePath;
		this.eventBufferList = eventBuffer;
	}
	
	public void run() {
		try {
			DatumReader<MailRecord> userDatumReader = new SpecificDatumReader<MailRecord>(MailRecord.class);
			DataFileReader<MailRecord> dataFileReader = new DataFileReader<MailRecord>(new File(filePath), userDatumReader);
			MailRecord email = null;

			long counter = 1;

			while (true) {
				//The following statement makes the code to iterate over the data set infinitely.
				if(!dataFileReader.hasNext()){
					dataFileReader = new DataFileReader<MailRecord>(new File(filePath), userDatumReader);
				}

				email = dataFileReader.next();

				this.eventBufferList.put(email);
<<<<<<< HEAD

				try{
					//String jsonInString = mapper.writeValueAsString(email);
					//System.out.println("string converted");
					KafkaMessageSender.runProducer(email.toString());

				}

				catch (NullPointerException e) {
					e.printStackTrace();
				}
				 catch (Exception e) {
					e.printStackTrace();
				}

=======
>>>>>>> c5799b2760711c3932d610cb5fc348c82d29aac4
			}
		}catch(IOException e){
			e.printStackTrace();
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
