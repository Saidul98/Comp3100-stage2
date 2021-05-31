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
		private Socket sc;
		private BufferedReader recievemsgServer;
		private DataOutputStream sendmsgServer;

		public ServerConnection() throws IOException {
			// Initialise server connection to localhost, and both read and write methods
			this.sc = new Socket("127.0.0.1", 50000);
			this.recievemsgServer = new BufferedReader(new InputStreamReader(sc.getInputStream()));
			this.sendmsgServer = new DataOutputStream(sc.getOutputStream());
		}

		private void sendmsg(String txt) throws IOException {
			sendmsgServer.write((txt + "\n").getBytes());
		}

		public List<String> readmsg() throws IOException {
			List<String> serverRespond = new ArrayList<String>();
			while(serverRespond.size() < 1) {
				while(this.recievemsgServer.ready()) {
					serverRespond.add(this.recievemsgServer.readLine());
				}
			}
			return serverRespond;
		}

		public void HELO() throws IOException {
			sendmsg("HELO");
			readmsg();
		}

		public void AUTH() throws IOException {
			sendmsg("AUTH " + System.getProperty("user.name"));
			readmsg();
		}

		public void REDY() throws IOException {
			sendmsg("REDY");
		}

 		public void SCHD(JOBN job, Server server) throws IOException {
			sendmsg("SCHD " + job.id + " " + server.type + " " + server.id);
		}

		public List<String> LSTJ(Server server) throws IOException {
			sendmsg("LSTJ " + server.type + " " + server.id);
			readmsg();
			sendmsg("OK");

			List<String> jobList = new ArrayList<String>();

			while(true) {
				String toAdd = readmsg().get(0);
				if(toAdd.equals(".")) {
					break;
				}
				jobList.add(toAdd);
				sendmsg("OK");
			}

			return jobList;
		}

		public void flush() throws IOException {
			sendmsgServer.flush();
		}

		public void close() throws IOException {
			sendmsg("QUIT");
			sendmsgServer.close();
			recievemsgServer.close();
			sc.close();
		}
	}

	static class Server {
		public String type;
		public int id;
		public int coreCount;
		public int memory;
		public int disk;
		List<JOBN> jobs;
		public int jobsScheduled;
		public int jobsScheduling;

		public Server(String type, int id, int coreCount, int memory, int disk) {
			this.type = type;
			this.id = id;
			this.coreCount = coreCount;
			this.memory = memory;
			this.disk = disk;
			this.jobs = new ArrayList<JOBN>();
			this.jobsScheduled = 0;
			this.jobsScheduling = 0;
		}

		public float workload(JOBN newJob) {
			if(this.jobs.size() == 0) {
				return 0;
			}

			int usedCore = 0;
			for(int i = 0; i < this.jobsScheduled; i++) {
				usedCore += this.jobs.get(i).core;
			}

			if(usedCore + newJob.core <= this.coreCount && this.jobsScheduling == 0) {
				return 0;
			} 

			float checkTime = 0;
			for(JOBN job: this.jobs) {
				checkTime += (float)job.estRuntime * ((float)job.core / this.coreCount);
			}

			return checkTime;
		}

		public boolean isCapable(JOBN job) {
			if(job.core <= this.coreCount && job.memory <= this.memory && job.disk <= this.disk) {
				return true;
			} else {
				return false;
			}
		}
 
		public void updateJobStats(ServerConnection SC) throws IOException {
			List<String> joblists = SC.LSTJ(this);

			List<JOBN> results = new ArrayList<JOBN>();
			this.jobsScheduled = 0;
			this.jobsScheduling = 0;

			for(String s: joblists) {
				String[] split = s.split(" ");
				String job = "0 0 " + split[0] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6];
				results.add(new JOBN(job));
				if(split[1].equals("2")) {
					this.jobsScheduled++;
				}
				if(split[1].equals("1")) {
					this.jobsScheduling++;
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

	public static void turnaroundTime(ServerConnection SC, Servers servers, JOBN job) throws IOException {
		int result = -1;

		for(int i = 0; i < servers.serverList.size(); i++) {
			if(servers.serverList.get(i).isCapable(job) && result == -1) {
				result = i;
			}
			if(servers.serverList.get(i).isCapable(job) && servers.serverList.get(i).workload(job) < servers.serverList.get(result).workload(job)) {
				result = i;
			}
		}

		SC.SCHD(job, servers.serverList.get(result));
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
				String serverResponse = SC.readmsg().get(0);
				String[] serverResponseSplit = serverResponse.split(" ");

				if (serverResponseSplit[0].equals("JOBN")) {
					JOBN job = new JOBN(serverResponse);
					servers.updateAllJobStats(SC);
					turnaroundTime(SC, servers, job);

					SC.readmsg();
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
