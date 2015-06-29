package output;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import jinmanager.JinManager;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Time;

import com.hubspot.jinjava.interpret.Context;

import scala.Tuple2;

public class Mail implements Function2 {
	static public final String defaultTransformation = "foreachRDD";
	private final String mailhost;
	private final String from;
	private final String[] toList;
	private final String subject;
	private final String format;

	private final String contentType;

	private final Boolean auth;
	private final String sender;
	private final String password;

	private final int tryTime;
	private final int interval;

	public Mail(HashMap conf) {
		System.out.println(conf);

		this.mailhost = (String) conf.get("mailhost");
		this.from = (String) conf.get("from_addr");
		ArrayList<String> t = (ArrayList<String>) conf.get("to_list");
		this.toList = new String[]{};
		t.toArray(this.toList);
		this.subject = (String) conf.get("subject");

		if (conf.containsKey("content_type"))
			this.contentType = (String) conf.get("contentType");
		else
			this.contentType = "text/plain";

		this.auth = (Boolean) conf.get("auth");
		this.sender = (String) conf.get("sender");
		this.password = (String) conf.get("password");

		if (conf.containsKey("try_time")) {
			this.tryTime = (int) conf.get("try_time");
		} else {
			this.tryTime = 1;
		}

		if (conf.containsKey("interval")) {
			this.interval = (int) conf.get("interval");
		} else {
			this.interval = 5;
		}

		if (conf.containsKey("format")) {
			String _format = "";
			for (String string : (ArrayList<String>) conf.get("format")) {
				_format += string + "\n";
			}
			this.format = _format;
		} else {
			this.format = "{{event}}";
		}
	}

	private void sendEmail(final String sender, String[] receivers,
			String subject, String mailContent) throws Exception {
		Properties props = new Properties();

		props.put("mail.smtp.host", this.mailhost);

		props.put("mail.smtp.auth", this.mailhost);

		Session session = null;
		if (this.auth) {
			Authenticator authenticator = new Authenticator() {
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication(sender, password);
				}
			};

			session = Session.getDefaultInstance(props, authenticator);
		} else {
			session = Session.getDefaultInstance(props);
		}

		MimeMessage mimeMessage = new MimeMessage(session);

		mimeMessage.setFrom(new InternetAddress(sender));

		InternetAddress[] receiver = new InternetAddress[receivers.length];
		for (int i = 0; i < receivers.length; i++) {
			receiver[i] = new InternetAddress(receivers[i]);
		}

		mimeMessage.setRecipients(Message.RecipientType.TO, receiver);
		mimeMessage.setSentDate(new Date());
		mimeMessage.setSubject(subject);

		Multipart multipart = new MimeMultipart();
		MimeBodyPart body = new MimeBodyPart();
		body.setContent(mailContent, this.contentType);

		multipart.addBodyPart(body);

		mimeMessage.setContent(multipart);

		Transport.send(mimeMessage);
	}

	@Override
	public Object call(Object arg0, Object arg1) {

		JavaPairRDD rdd = (JavaPairRDD) arg0;

		rdd.foreachPartition(new VoidFunction<Iterator>() {

			@Override
			public void call(Iterator iter) throws Exception {

				String msg = "";

				while (iter.hasNext()) {
					final Tuple2 e = (Tuple2) iter.next();

					final ArrayList event = new ArrayList() {
						{
							add(e._1);
							add(e._2);
						}
					};

					HashMap binding = new HashMap() {
						{
							put("event", event);
						}
					};

					Context cc = new Context(JinManager.c, binding);

					msg += JinManager.jinjava.render(format, cc);

				}

				System.out.print(msg);

				int try_count = 0;
				while (try_count < tryTime) {
					try {
						sendEmail(from, toList, subject, msg);
						return;
					} catch (Exception e) {
						e.printStackTrace();
						try_count++;
						Thread.sleep(interval);
					}
				}

				System.out.println("could not send");
				return;
			}

		});
		return null;
	}
}
