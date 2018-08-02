package org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor;

import com.uebercomputing.mailrecord.MailRecord;

import javax.mail.internet.MimeUtility;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by miyurud on 4/16/15.
 */
public class FilterOperator {
    private String ENRON_DOMAIN = "enron.com";

    public MailRecord process(MailRecord obj) {
        String fromAddress = obj.get("from").toString();

        //-------- task 1 ----------------------------------------------------------------------------------------------
        //Drop emails that did not originate within Enron, i.e., email addresses that do not end
        //with @enron.com or with @enron.com>
        if((!(fromAddress.endsWith(ENRON_DOMAIN)))){
            return null;
        }

        //-------- task 2 ----------------------------------------------------------------------------------------------
        //remove all
        //email addresses that do not end with @enron.com from the To, CC, and BCC fields.
        List<CharSequence> toAddressesList = obj.getTo();
        List<CharSequence> ccAddressesList = obj.getCc();
        List<CharSequence> bccAddressesList = obj.getBcc();

        boolean flag = false;

        //To addresses
        if(toAddressesList != null){
            List<CharSequence> toAddressesListNew = new LinkedList<CharSequence>();
            Iterator<CharSequence> itr = toAddressesList.iterator();

            CharSequence item = null;

            //We have to iterate through the To email addresses and see whether those contain any email address which is
            //outside the "enron.com" domain. If so we have to remove that email address.

            while (itr.hasNext()) {
                item = itr.next();

                if ((((String) item).endsWith(ENRON_DOMAIN))) {
                    toAddressesListNew.add(item.toString());
                } else {
                    flag = true;
                }
            }

            obj.setTo(toAddressesListNew);
        }

        //CC addresses
        if(ccAddressesList != null){
            List<CharSequence> ccAddressesListNew = new LinkedList<CharSequence>();
            Iterator<CharSequence> itr = ccAddressesList.iterator();

            CharSequence item = null;

            //We have to iterate through the To email addresses and see whether those contain any email address which is
            //outside the "enron.com" domain. If so we have to remove that email address.

            while (itr.hasNext()) {
                item = itr.next();

                if ((((String) item).endsWith(ENRON_DOMAIN))) {
                    ccAddressesListNew.add(item.toString());
                } else {
                    flag = true;
                }
            }

            obj.setCc(ccAddressesListNew);
        }

        //BCC addresses
        if(bccAddressesList != null){
            List<CharSequence> bccAddressesListNew = new LinkedList<CharSequence>();
            Iterator<CharSequence> itr = bccAddressesList.iterator();

            CharSequence item = null;

            //We have to iterate through the To email addresses and see whether those contain any email address which is
            //outside the "enron.com" domain. If so we have to remove that email address.

            while (itr.hasNext()) {
                item = itr.next();

                if ((((String) item).endsWith(ENRON_DOMAIN))) {
                    bccAddressesListNew.add(item.toString());
                } else {
                    flag = true;
                }
            }

            obj.setBcc(bccAddressesList);
        }

        //-------- task 3 ----------------------------------------------------------------------------------------------
        //we need to remove rogue formatting such as dangling newline characters
        //and MIME quoted-printable characters
        //from the email body to restrict the character set to simple ASCII
        String body = obj.getBody().toString();
        //remove dangling new lines
        body = body.trim();

        //Remove any MIME encoded text.
        try {
            body = MimeUtility.decodeText(body);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        obj.setBody(body);

        return obj;
    }
}