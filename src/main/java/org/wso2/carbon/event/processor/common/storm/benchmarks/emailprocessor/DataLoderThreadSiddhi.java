/**
 * 
 */
package org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor;

import com.uebercomputing.mailrecord.MailRecord;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor.util.Constants;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Miyuru Dayarathna
 *
 */
public class DataLoderThreadSiddhi extends Thread {
	private String filePath;
//	private LinkedBlockingQueue<Object> eventBufferList;
	private BufferedReader br;
	private int count;
	private String schemaPath = "";
	private InputHandler inputHandler;
	volatile long events = 0;
	private long startTime;
	private long differenceFromNTP = 0l; //This is the time drift from the original NTP server.
	private int NITR = 10;
	private boolean firstItemFlag = true;

	public DataLoderThreadSiddhi(String filePath, LinkedBlockingQueue<Object> eventBuffer, InputHandler inputHandler, long differenceFromNTP){
        super("Data Loader");
        this.filePath = filePath;
//		this.eventBufferList = eventBuffer;
		this.inputHandler = inputHandler;
		//In this version of the benchmark, the CEP server will be working as the starting and the ending points.
		this.differenceFromNTP = differenceFromNTP;//getAverageTimeDifference(NITR);
		System.out.println("differenceFromNTP : " + differenceFromNTP);
	}

	public DataLoderThreadSiddhi(String filePath, InputHandler inputHandler, long differenceFromNTP){
		super("Data Loader");
		this.filePath = filePath;
		this.inputHandler = inputHandler;
		this.differenceFromNTP = differenceFromNTP;
		System.out.println("differenceFromNTP : " + differenceFromNTP);
	}

	public void run() {
		Object[] event = null;
		try {
			DatumReader<MailRecord> userDatumReader = new SpecificDatumReader<MailRecord>(MailRecord.class);
			DataFileReader<MailRecord> dataFileReader = new DataFileReader<MailRecord>(new File(filePath), userDatumReader);
			MailRecord email = null;

			startTime = System.currentTimeMillis();
			while (true) {
				//The following statement makes the code to iterate over the data set infinitely.
//				if(!dataFileReader.hasNext()){
//					dataFileReader = new DataFileReader<MailRecord>(new File(filePath), userDatumReader);
//				}

				if(!dataFileReader.hasNext()){
					event = new Object[8];
					event[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = -1l;
					event[1] = "miyurud@enron.com";
					event[2] = "miyurud@enron.com";
					event[3] = "miyurud@enron.com";
					event[4] = "miyurud@enron.com";
					event[5] = "miyurud@enron.com";
					event[6] = "final-tuple";
					event[7] = ""+events;

					try {
						this.inputHandler.send(event);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					break;
				}

				//The very first event injected will be a synthetic one which will help to determine the total end-to-end elapsed time.
				if(firstItemFlag){
					firstItemFlag = false;

					event = new Object[8];
					event[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = -2l;
					event[1] = "miyurud@enron.com";
					event[2] = "miyurud@enron.com";
					event[3] = "miyurud@enron.com";
					event[4] = "miyurud@enron.com";
					event[5] = "miyurud@enron.com";
					event[6] = "final-tuple";
					event[7] = Long.toString(System.currentTimeMillis() + differenceFromNTP);

					try {
						this.inputHandler.send(event);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					continue;
				}

				email = dataFileReader.next();

				events++;

				//We are interested of only on the following set of fields.getBody();.
				//fromAddresses, toAddresses, ccAddress, bccAddresses, subject, body
				event = new Object[8];//We need one additional field for timestamp.
				long cTime = System.currentTimeMillis() + differenceFromNTP;
				event[Constants.INPUT_INJECTION_TIMESTAMP_FIELD]	= cTime;
				ArrayList fromAddresses = new ArrayList();

				//event[1] = split(email.getFrom());
				event[1] = email.getFrom();
				Iterator<CharSequence> itr = null;
				int counter = 0;
				StringBuilder sb = new StringBuilder();

				final List<CharSequence> to = email.getTo();
				if(to != null) {
					itr = to.iterator();

					while (itr.hasNext()) {
//						((Object[]) event[2])[counter] = itr.next();
						sb.append(itr.next());
						if(itr.hasNext()){
							sb.append(",");
						}
						counter++;
					}
				}

				event[2] = sb.toString();
				sb = new StringBuilder();

				final List<CharSequence> cc = email.getCc();
				if(cc != null) {
					//event[3] = new Object[cc.size()];
					itr = cc.iterator();
					counter = 0;

					while (itr.hasNext()) {
						//((Object[]) event[3])[counter] = itr.next();
						sb.append(itr.next());
						if(itr.hasNext()){
							sb.append(",");
						}
						counter++;
					}
				}

				event[3] = sb.toString();
				sb = new StringBuilder();

				final List<CharSequence> bcc = email.getBcc();
				if(bcc != null) {
					//event[4] = new Object[bcc.size()];
					itr = bcc.iterator();
					counter = 0;

					while (itr.hasNext()) {
						//((Object[]) event[4])[counter] = itr.next();
						sb.append(itr.next());
						if(itr.hasNext()){
							sb.append(",");
						}
						counter++;
					}
				}

				event[4] = sb.toString();
				event[5] = email.getSubject();

				//System.out.println(""+events+","+email.getBody().toString().getBytes().length+","+email.getBody().toString().length());

				event[6] = email.getBody();
				event[7] = "(.*)@enron.com";

				try {
					this.inputHandler.send(event);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

//			long currentTime = System.currentTimeMillis();
//			System.out.println("****** Input ******");
//			System.out.println("events read : " + events);
//			System.out.println("time to read (ms) : " + (currentTime - startTime));
//			System.out.println("read throughput (events/s) : " + (events * 1000 / (currentTime - startTime)));
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	public Object[] split(CharSequence item){
		int itemLen = item.length();
		int counter = 0;
		Object[] result = null;

		for(int i = 0; i < itemLen; i++){
			if(item.charAt(i) == ','){
				counter++;
			}
		}

		result = new Object[counter];
		counter = 0;
		int counter2 = 0;

		for(int i = 0; i < itemLen; i++) {
			if (item.charAt(i) == ',') {
				result[counter2] = item.subSequence(counter, i);
				counter = i;
				counter2++;
			}
		}

		return result;
	}
}
