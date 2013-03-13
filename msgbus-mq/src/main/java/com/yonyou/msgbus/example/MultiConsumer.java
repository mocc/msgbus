package com.yonyou.msgbus.example;

public class MultiConsumer extends Thread {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		AsyncConsumer c = null;

		c = new AsyncConsumer();

		String[] myArgs = new String[3];
		myArgs[0] = "test";
		myArgs[1] = "0";
		myArgs[2] = "5000";

		try {
			c.recv(myArgs);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("receiving finished.");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//PropertyConfigurator.configure("F:/Java Projects2/mq-test/log4j.properties");
		MultiConsumer m1 = new MultiConsumer();
		MultiConsumer m2 = new MultiConsumer();
		

		m1.start();
		m2.start();
		
	}

}
