public class DriverClass {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String brokerId = args[0];
		String topicName = args[1];
		String groupId = args[2];
		String chkpointDir = args[3];
		int analysisNo = Integer.parseInt(args[4]);
		
		String[] p = {brokerId, topicName, groupId, chkpointDir };
		if (analysisNo == 1) {
			try {
				SparkKafka.main(p);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else if (analysisNo == 2) {
			try {
				SparkKafkaQ2.main(p);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else if (analysisNo == 3) {
			try {
				SparkKafkaQ3.main(p);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
