import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Client {

	static class ServerConnection {
		private Socket s;
		private BufferedReader recieveFromServer;
		private DataOutputStream sendToServer;

		public ServerConnection() throws IOException {
			// Initialise server connection to localhost, and both read and write methods
			this.s = new Socket("127.0.0.1", 50000);
			this.recieveFromServer = new BufferedReader(new InputStreamReader(s.getInputStream()));
			this.sendToServer = new DataOutputStream(s.getOutputStream());
		}

		private void send(String msg) throws IOException {
			sendToServer.write((msg + "\n").getBytes());
		}

		public List<String> read() throws IOException {
			List<String> serverReplies = new ArrayList<String>();
			while(serverReplies.size() < 1) {
				while(this.recieveFromServer.ready()) {
					serverReplies.add(this.recieveFromServer.readLine());
				}
			}
			return serverReplies;
		}

		public void HELO() throws IOException {
			send("HELO");
			read();
		}

		public void AUTH() throws IOException {
			send("AUTH " + System.getProperty("user.name"));
			read();
		}

		public void REDY() throws IOException {
			send("REDY");
		}

		public void SCHD(JOBN job, Server server) throws IOException {
			send("SCHD " + job.id + " " + server.type + " " + server.id);
		}

		public List<String> LSTJ(Server server) throws IOException {
			send("LSTJ " + server.type + " " + server.id);
			read();
			send("OK");

			List<String> jobStrings = new ArrayList<String>();

			while(true) {
				String toAdd = read().get(0);
				if(toAdd.equals(".")) {
					break;
				}
				jobStrings.add(toAdd);
				send("OK");
			}

			return jobStrings;
		}

		public void flush() throws IOException {
			sendToServer.flush();
		}

		public void close() throws IOException {
			send("QUIT");
			sendToServer.close();
			recieveFromServer.close();
			s.close();
		}
	}

	static class Server {
		public String type;
		public int id;
		public int coreCount;
		public int memory;
		public int disk;
		List<JOBN> jobs;
		public int jobsRunning;
		public int jobsWaiting;

		public Server(String type, int id, int coreCount, int memory, int disk) {
			this.type = type;
			this.id = id;
			this.coreCount = coreCount;
			this.memory = memory;
			this.disk = disk;
			this.jobs = new ArrayList<JOBN>();
			this.jobsRunning = 0;
			this.jobsWaiting = 0;
		}

		public float workload(JOBN newJob) {
			if(this.jobs.size() == 0) {
				return 0;
			}

			int runningCoreTotal = 0;
			for(int i = 0; i < this.jobsRunning; i++) {
				runningCoreTotal += this.jobs.get(i).core;
			}

			if(runningCoreTotal + newJob.core <= this.coreCount && this.jobsWaiting == 0) {
				return 0;
			}

			float timeTest = 0;
			for(JOBN job: this.jobs) {
				timeTest += (float)job.estRuntime * ((float)job.core / this.coreCount);
			}

			return timeTest;
		}

		public boolean isCapable(JOBN job) {
			if(job.core <= this.coreCount && job.memory <= this.memory && job.disk <= this.disk) {
				return true;
			} else {
				return false;
			}
		}

		public void updateJobStats(ServerConnection SC) throws IOException {
			List<String> jobStrings = SC.LSTJ(this);

			List<JOBN> results = new ArrayList<JOBN>();
			this.jobsRunning = 0;
			this.jobsWaiting = 0;

			for(String s: jobStrings) {
				String[] split = s.split(" ");
				String jobString = "0 0 " + split[0] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6];
				results.add(new JOBN(jobString));
				if(split[1].equals("2")) {
					this.jobsRunning++;
				}
				if(split[1].equals("1")) {
					this.jobsWaiting++;
				}
			}

			this.jobs = results;
		}
	}

	static class Servers {
		public List<Server> serverList;

		public Servers() {
			this.serverList = new ArrayList<Server>();
		}

		public void updateAllJobStats(ServerConnection SC) throws IOException {
			for(Server s: this.serverList) {
				s.updateJobStats(SC);
			}
		}

		public void importXML(String fileLocation) throws ParserConfigurationException, IOException, SAXException {
			File dssystemxml = new File(fileLocation);
	
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
			Document doc = docBuilder.parse(dssystemxml);
	
			doc.getDocumentElement().normalize();
	
			NodeList servers = doc.getElementsByTagName("server");

			// Add each server in the XML server to a class instance, to be added to an array
			for(int i = 0; i < servers.getLength(); i++) {
				Element server = (Element) servers.item(i);
	
				String t = server.getAttribute("type");
				int l = Integer.parseInt(server.getAttribute("limit"));
				int c = Integer.parseInt(server.getAttribute("coreCount"));
				int m = Integer.parseInt(server.getAttribute("memory"));
				int d = Integer.parseInt(server.getAttribute("disk"));

				for(int j = 0; j < l; j++) {
					this.serverList.add(new Server(t, j, c, m, d));
				}
			}
		}
	}

	static class JOBN {
		public int id;
		public int estRuntime;
		public int core;
		public int memory;
		public int disk;

		public JOBN(String msg) {
			String[] splitString = msg.split(" ");
			this.id = Integer.parseInt(splitString[2]);
			this.estRuntime = Integer.parseInt(splitString[3]);
			this.core = Integer.parseInt(splitString[4]);
			this.memory = Integer.parseInt(splitString[5]);
			this.disk = Integer.parseInt(splitString[6]);
		}
	}

	public static void TTBiasAlgorithm(ServerConnection SC, Servers servers, JOBN job) throws IOException {
		int resultIndex = -1;

		for(int i = 0; i < servers.serverList.size(); i++) {
			if(servers.serverList.get(i).isCapable(job) && resultIndex == -1) {
				resultIndex = i;
			}
			if(servers.serverList.get(i).isCapable(job) && servers.serverList.get(i).workload(job) < servers.serverList.get(resultIndex).workload(job)) {
				resultIndex = i;
			}
		}

		SC.SCHD(job, servers.serverList.get(resultIndex));
	}
	
	public static void main(String[] args) {
		try {
			ServerConnection SC = new ServerConnection();

			// Initial server message transfer
			SC.HELO();

			SC.AUTH();

			Servers servers = new Servers();
			servers.importXML("./ds-system.xml");

			runner: while (true) {
				SC.REDY();
				String serverResponse = SC.read().get(0);
				String[] serverResponseSplit = serverResponse.split(" ");

				if (serverResponseSplit[0].equals("JOBN")) {
					JOBN job = new JOBN(serverResponse);
					servers.updateAllJobStats(SC);
					TTBiasAlgorithm(SC, servers, job);

					SC.read();
					}

				if (serverResponseSplit[0].equals("NONE")) {
					break runner;
				}
			}

			SC.flush();
			SC.close(); 
		} catch (Exception e) {
			System.out.println(e);
		}
	}  
}