package uwdb.discovery.dependency.approximate.search;

public class MiningMinMVDsUnitTests {
	
	public static void main(String[] args) {
		double[] thresholds = new double[] {0,0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5,
     		0.55,0.6,0.65,0.7,0.75,0.8,0.9,1.0,1.1,1.15,1.3,1.4,1.5,1.6,2.0,2.5,3,24};
		//unit test on small, simple DBs
		String balance = "C:\\Users\\batyak\\Dropbox\\postdoc\\2019\\DataForTesting\\Easy\\balance-scale.csv";
		Integer balanceNumAtts=5;
		String[] balanceArgs = new String[] {balance, balanceNumAtts.toString()};
		String abalon = "C:\\Users\\batyak\\Dropbox\\postdoc\\2019\\DataForTesting\\Easy\\abalone.csv";
		Integer abalonNumAtts=9;
		String[] abalonArgs = new String[] {abalon, abalonNumAtts.toString()};
		String chess = "C:\\Users\\batyak\\Dropbox\\postdoc\\2019\\DataForTesting\\Easy\\chess.csv";
		Integer chessNumAtts=7;
		String[] chessArgs = new String[] {chess, chessNumAtts.toString()};
		
		int numPassed=0;		
		for(double thresh: thresholds){
			boolean currTest = MinimalJDGenerator.testMinSeps(balanceArgs, thresh);
			if(!currTest) {
				System.out.println("Balance test failed for threshold " + thresh);				
			}
			else {
				numPassed++;
			}
		}
		System.out.println("[Balance]: passed " + numPassed + " out of " + thresholds.length);
		
		numPassed=0;	
		for(double thresh: thresholds){
			boolean currTest =MinimalJDGenerator.testMinSeps(abalonArgs, thresh);
			if(!currTest) {
				System.out.println("Abalon test failed for threshold " + thresh);				
			}
			else {
				numPassed++;
			}
		}
		System.out.println("[Abalon]: passed " + numPassed + " out of " + thresholds.length);
		
		numPassed=0;	
		for(double thresh: thresholds){
			boolean currTest =MinimalJDGenerator.testMinSeps(chessArgs, thresh);
			if(!currTest) {
				System.out.println("Chess test failed for threshold " + thresh);				
			}
			else {
				numPassed++;
			}
		}
		System.out.println("[Chess]: passed " + numPassed + " out of " + thresholds.length);
	 
	}
}
