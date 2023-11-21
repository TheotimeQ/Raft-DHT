package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

//------------------------------------------------------------------------------------------------------
//------------------------------------------------RAFT--------------------------------------------------
//------------------------------------------------------------------------------------------------------

// ------------------------Config Nodes-------------------
type NodeConfig struct {
	PortRaft string `json:"portraft"`
	IP       string `json:"ip"`
	PortDHT  string `json:"portdht"`
}

//------------------------Paramettre programme---------------------

var slowFactor = 30

//----------------------NODE LOGIC----------------------

// Node est une structure qui contient les informations d'une node
type Node struct {
	ID   int
	Ip   string
	Port string

	State           string
	TimeoutElection int
	HeartBeatRate   int
	LastElection    time.Time
	LastHeartBeat   time.Time

	Term       int
	VoteCount  int
	leaderPort int
}

// NewNode crée une nouvelle node
func NewNode(id int) *Node {
	return &Node{

		ID:   id,
		Ip:   nodeConfig[id].IP,
		Port: nodeConfig[id].PortRaft,

		State:           "Follower",
		TimeoutElection: 0,
		Term:            0,
		VoteCount:       0,
		leaderPort:      -1,
	}
}

func formatString(input string, length int) string {
	if len(input) >= length {
		return input[:length]
	}
	return input + " " + formatString("", length-len(input)-1)
}

// Create fileliste if not exist :
func createFileList(id int) {
	if _, err := os.Stat("SharedFileList/List_" + strconv.Itoa(id) + ".txt"); os.IsNotExist(err) {
		// create file
		var file, err = os.Create("SharedFileList/List_" + strconv.Itoa(id) + ".txt")
		if err != nil {
			fmt.Println(err)
			return
		}
		defer file.Close()
	}
}

// remove une ligne d'un fichier
func removeLineWithString(fileName string, lineToRemove string) error {
	file, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	tmpFile, err := os.CreateTemp("", "tempFile")
	if err != nil {
		return err
	}
	defer tmpFile.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line != lineToRemove {
			_, err := fmt.Fprintf(tmpFile, "%s\n", line)
			if err != nil {
				return err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	file.Close()
	tmpFile.Close()
	if err := os.Rename(tmpFile.Name(), fileName); err != nil {
		return err
	}

	return nil
}

// Print node info
func (n *Node) PrintInfoReceive(command string, IP string, Port string, Term string) {

	statestr := formatString(n.State, 10)
	termstr := formatString(strconv.Itoa(n.Term), 3)
	command = formatString(command, 22)
	fmt.Println("Node " + strconv.Itoa(n.ID) + " " + statestr + "| Term " + termstr + " || " + command + " - " + n.Ip + ":" + n.Port + " - term " + Term)
}

// Ouvre un fichier et renvoi tout son contenu ren return content string
func getcontent(filename string) string {

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	content := ""
	for scanner.Scan() {
		content += scanner.Text() + ","
	}

	return content
}

// Ajoute dans le fichier
func addInFile(filename string, filetoadd string) {

	//Ajoute filetoadd dans le fichier SharedFileList/List_ID.txt
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	if _, err = f.WriteString(filetoadd + "\n"); err != nil {
		panic(err)
	}

	defer f.Close()
}

// HandleRequest est la fonction qui gère les requêtes reçues par la node
func (n *Node) handleRequest(w http.ResponseWriter, r *http.Request) {

	// On récupère l'adresse IP de l'expéditeur
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		log.Println(err)
		return
	}

	// On récupère le port de l'expéditeur
	port := r.URL.Query().Get("port")
	term := r.URL.Query().Get("term")

	//Si revoit un message sur /vote, il vote pour la node qui lui a envoye le message
	if r.URL.Path == "/requestvote" {

		n.PrintInfoReceive("requestvote", ip, port, term)

		//Si la node est Follower, elle vote
		if n.State == "Follower" {

			//Update term
			n.Term++

			//Send vote
			n.sendMessage(ip, port, n.Term, "vote", "ok")

			//Reset timeout
			n.LastElection = time.Now()
		}
	}

	//Si recoit un message sur /vote, il incremente son compteur de vote
	if r.URL.Path == "/vote" {

		if n.State == "Candidate" {

			//Si le term est le meme que le sien, il incremente son compteur de vote
			if term == strconv.Itoa(n.Term) {
				//Envoi le term + vote
				n.PrintInfoReceive("vote", ip, port, term)

				//Increment vote count
				n.VoteCount++
			}

			n.leaderPort = -1
		}
	}

	//Si revoit un message sur /heartbeat, il reinitialise son timeout
	if r.URL.Path == "/heartbeat" {
		n.PrintInfoReceive("heartbeat", ip, port, term)

		if n.State == "Follower" {
			//Repond au heartbeat
			n.sendMessage(ip, port, n.Term, "heartbeatresponse", "ok")

			//Reset timeout
			n.LastElection = time.Now()

			n.leaderPort, _ = strconv.Atoi(port)
		}
	}

	//Si revoit un message sur /heartbeatresponse, il reinitialise son timeout
	if r.URL.Path == "/heartbeatresponse" {
		n.PrintInfoReceive("heartbeatresponse", ip, port, term)

		if n.State == "Follower" {

			//Reset timeout
			n.LastElection = time.Now()

		}
	}

	//Si revoit une commande /add
	if r.URL.Path == "/add" {

		n.PrintInfoReceive("add", ip, port, term)
		filetoadd := r.URL.Query().Get("message")
		fmt.Println("Adding to file list : ", filetoadd)

		//Si le fichier SharedFileList/List_ID.txt existe pas
		createFileList(n.ID)

		//Ajoute une ligne au fichier
		addInFile("SharedFileList/List_"+strconv.Itoa(n.ID)+".txt", filetoadd)

	}

	//Si revoit une commande /del
	if r.URL.Path == "/del" {
		n.PrintInfoReceive("del", ip, port, term)
		filename := r.URL.Query().Get("message")
		fmt.Println("Deletin from file list : ", filename)

		//Si le fichier SharedFileList/List_ID.txt existe pas
		createFileList(n.ID)

		//Supprime file_id dans le fichier SharedFileList/List_ID.txt
		err := removeLineWithString("SharedFileList/List_"+strconv.Itoa(n.ID)+".txt", filename)
		if err != nil {
			fmt.Println(err)
		}
	}

	//Si revoit getall
	if r.URL.Path == "/getall" {
		fmt.Println("Sending all file")

		//repond à la requette initial avec le contenu du fichier
		content := "FileList" + "|" + getcontent("SharedFileList/List_"+strconv.Itoa(n.ID)+".txt")
		SendRequest(content, "127.0.0.1:300")
	}

	//if receive cmd from client ( /cmd )
	if r.URL.Path == "/cmd" {
		cmdtype := r.URL.Query().Get("cmdtype")
		message := r.URL.Query().Get("message")
		//Si folower , transmet la commande au leader
		if n.State == "Follower" {
			if n.leaderPort != -1 {
				fmt.Println("Not the leader , transmiting to leader : cmd", cmdtype, " ", message)
				_, err := http.Get("http://" + n.Ip + ":" + strconv.Itoa(n.leaderPort) + "/cmd" + "?cmdtype=" + cmdtype + "&message=" + message + "&port=" + n.Port + "&term=" + strconv.Itoa(n.Term))
				if err != nil {
					log.Println(err)
					return
				}
			} else {
				fmt.Println("Transmiting to leader")
			}
		}

		if n.State == "Leader" {
			fmt.Println("Broadcast message : " + cmdtype + " " + message + " to all nodes")

			//A tout le monde
			n.broadcastMessage(cmdtype, message)

			//A soit meme
			n.sendMessage(n.Ip, n.Port, n.Term, cmdtype, message)
		}
	}

}

// Envoi un message à une node
func (n *Node) sendMessage(ip string, port string, term int, endpoint string, message string) {

	_, err := http.Get("http://" + ip + ":" + port + "/" + endpoint + "?message=" + message + "&port=" + n.Port + "&term=" + strconv.Itoa(n.Term))
	if err != nil {
		log.Println(err)
		return
	}
}

// Envoi un message à toutes les nodes du cluster
func (n *Node) broadcastMessage(endpoint string, message string) {

	for i := 0; i < len(nodeConfig); i++ {
		if i == n.ID {
			continue
		}
		n.sendMessage(nodeConfig[i].IP, nodeConfig[i].PortRaft, n.Term, endpoint, message)
	}

}

// Start démarre le serveur de la node
func (n *Node) Start() {

	// Création du serveur
	srv := &http.Server{
		Addr:         ":" + n.Port,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Création du routeur
	mux := http.NewServeMux()
	mux.HandleFunc("/", n.handleRequest)

	// Lancement du serveur d'ecoute
	srv.Handler = mux
	go func() {
		fmt.Println("Node " + strconv.Itoa(n.ID) + " listening on port " + n.Port)
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	//Delay to have time to start other nodes
	time.Sleep(15 * time.Second)

	//Generate random number between 150 and 300 milliseconds
	n.TimeoutElection = rand.Intn(150*slowFactor) + 150*slowFactor
	n.HeartBeatRate = 50 * slowFactor
	n.LastElection = time.Now()
	n.LastHeartBeat = time.Now()

	// startedat := time.Now()

	//Boucle qui permet de changer l'etat de la node et de gerer les different comportement
	for {

		if n.State == "Follower" {

			//Si le timeout est depasse, la node devient candidate
			if time.Since(n.LastElection).Seconds() > float64(n.TimeoutElection)*0.001 {

				//Increment le term
				n.Term++

				n.State = "Candidate"
				fmt.Println("Node " + strconv.Itoa(n.ID) + " became candidate with election timeout: " + strconv.Itoa(n.TimeoutElection) + " ms")

				//Vote for himself
				n.VoteCount = 1

				//Send request vote to all nodes
				fmt.Println("Node " + strconv.Itoa(n.ID) + " sent requestvote")
				n.broadcastMessage("requestvote", "ok")

				//Reset timeout
				n.LastElection = time.Now()
			}
		}

		if n.State == "Candidate" {

			//Si majorete des votes, devient leader
			if n.VoteCount > len(nodeConfig)/2 {
				fmt.Println("Node " + strconv.Itoa(n.ID) + " became leader with " + strconv.Itoa(n.VoteCount) + " votes")
				n.State = "Leader"
				n.VoteCount = 0
			}
		}

		if n.State == "Leader" {

			//Send append entries to all nodes ( interval by timeout heartbeat)
			if time.Since(n.LastHeartBeat).Seconds() > float64(n.HeartBeatRate)*0.001 {
				fmt.Println("Node " + strconv.Itoa(n.ID) + " sent heartbeat")
				n.broadcastMessage("heartbeat", "ok")
				n.LastHeartBeat = time.Now()
			}
		}

		// //Si on est ID 0 et que 30 sec de passé , on ferme le processus
		// if n.ID == 0 {
		// 	if time.Since(startedat).Seconds() > 20 {
		// 		fmt.Println("Node " + strconv.Itoa(n.ID) + " stopped")
		// 		srv.Close()
		// 		return
		// 	}
		// }

	}
}

//------------------------------------------------------------------------------------------------------
//-------------------------------------------------DHT--------------------------------------------------
//------------------------------------------------------------------------------------------------------

// Recupere le contenu d'un fichier
func GetFirstLine(file *os.File) (string, error) {

	scanner := bufio.NewScanner(file)

	if scanner.Scan() {
		content := scanner.Text()
		return content, nil
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	return "", nil
}

func StartTCP(nodeID int, next_IP map[string]string, Letters string) {

	listener, err := net.Listen("tcp", nodeConfig[nodeID].IP+":"+nodeConfig[nodeID].PortDHT)
	if err != nil {
		fmt.Printf("Erreur lors de la création de l'écouteur : %v\n", err)
		return
	}
	defer listener.Close()

	fmt.Println("Serveur TCP en cours d'écoute sur 127.0.0.1:100", nodeConfig[nodeID].IP+":"+nodeConfig[nodeID].PortDHT)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Erreur lors de l'acceptation de la connexion : %v\n", err)
			continue
		}
		go handleCon(conn, Letters, next_IP, strconv.Itoa(nodeID))
	}
}

func CharRange(startChar, endChar rune) string {
	if startChar > endChar {
		startChar, endChar = endChar, startChar
	}

	var charList []string
	for char := startChar; char <= endChar; char++ {
		charList = append(charList, string(char))
	}

	return strings.Join(charList, "")
}

func ReadFileAndCreateMap(filename string) (map[string]string, error) {
	charRangeMap := make(map[string]string)

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " : ", 2)
		if len(parts) != 2 {
			continue
		}

		charRange := CharRange(rune(parts[0][0]), rune(parts[0][2]))
		value := strings.TrimSpace(parts[1])

		charRangeMap[charRange] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return charRangeMap, nil
}

// Charge le fichier en memoire pour l'envoyer par la suite en TCP
func GetFileToSend(File string, Letters string) (string, string) {

	//Si first letter est dans les lettre du server
	if strings.Contains(Letters, string(File[0])) == false {
		return "", "File not here"
	}

	file, err := os.Open("SharedFiles/" + File)
	if err != nil {
		return "", "File not found"
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	content := ""
	for scanner.Scan() {
		content += scanner.Text() + "\n"
	}

	return content, ""
}

func SendRequest(message string, next_IP string) error {
	conn, err := net.Dial("tcp", next_IP)
	if err != nil {
		fmt.Printf("Erreur lors de la connexion au serveur : %v\n", err)
		return err
	}
	defer conn.Close()
	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Printf("Erreur lors de l'envoi du message : %v\n", err)
		return err
	}
	fmt.Println("Message : " + message + " envoyé à " + next_IP)
	return nil
}

func FindNextHope(next_IP map[string]string, File string) string {

	for key, value := range next_IP {
		if strings.Contains(key, string(File[0])) {
			return value
		}
	}
	return ""
}

func handleCon(conn net.Conn, Letters string, next_IP map[string]string, nodeID string) {
	defer conn.Close()

	buffer := make([]byte, 1024)
	for {

		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Printf("Erreur lors de la lecture depuis la connexion : %v\n", err)
			return
		}

		data := string(buffer[:n])
		fmt.Printf("Données reçues : %s", data+"\n")
		split := strings.Split(data, "|")
		if len(split) == 4 {
			Origin := split[0]
			File := split[1]
			MaxDepth, _ := strconv.Atoi(split[2])
			Depth, _ := strconv.Atoi(split[3])
			filedata, err_search := GetFileToSend(File, Letters)

			//Si pas d'erreur on envoie le fichier à l'origine en TCP
			if err_search == "" {
				message := "Found" + "|" + File + "|" + nodeID + "|" + filedata
				err = SendRequest(message, Origin)
				if err != nil {
					fmt.Printf("Fichier %s envoyé au server : %s\n", File, Origin)
				}
				return
			}
			//Si on a pas trouvé le fichier on le transmet au prochain server
			if err_search == "File not here" {
				if Depth < MaxDepth {
					message := Origin + "|" + File + "|" + strconv.Itoa(MaxDepth) + "|" + strconv.Itoa(Depth+1)
					nextHope := FindNextHope(next_IP, File)
					if nextHope == "" {
						message := "File not found" + "|" + File
						err = SendRequest(message, Origin)
						if err != nil {
							fmt.Printf("No server gere this file : %s\n", Origin)
						}
						return
					}
					err = SendRequest(message, nextHope)
					if err != nil {
						fmt.Printf("Requette forward au server : %s\n", nextHope)
					}
					return

				} else {
					message := "Not found after max depth" + "|" + File
					err = SendRequest(message, Origin)
					if err != nil {
						fmt.Printf("Max depth error sent to origin : %s\n", Origin)
					}
					return
				}
			}
			if err_search == "File not found" {
				message := "File not found" + "|" + File
				err = SendRequest(message, Origin)
				if err != nil {
					fmt.Printf("File not found error sent to origin : %s\n", Origin)
				}
				return
			}
			return
		}
	}
}

//------------------------------------------------------------------------------------------------------
//----------------------------------------------MAIN--------------------------------------------------
//------------------------------------------------------------------------------------------------------

var nodeConfig = map[int]NodeConfig{
	0: {PortRaft: "8000", IP: "127.0.0.1", PortDHT: "1000"},
	1: {PortRaft: "8001", IP: "127.0.0.1", PortDHT: "1001"},
	2: {PortRaft: "8002", IP: "127.0.0.1", PortDHT: "1002"},
	3: {PortRaft: "8003", IP: "127.0.0.1", PortDHT: "1003"},
	// 4: {PortRaft: "8004", IP: "127.0.0.1", PortDHT: "1004"},
	// 5: {PortRaft: "8005", IP: "127.0.0.1", PortDHT: "1005"},
}

func main() {

	if len(os.Args) < 2 {
		fmt.Println("go run main.go <nodeID>")
		return
	}
	nodeIDInt, _ := strconv.Atoi(os.Args[1])
	var nodeIDStr = os.Args[1]

	//---------------------START RAFT----------------------

	NodesRaft := NewNode(nodeIDInt)

	//Wait 10 sec before starting raft
	go NodesRaft.Start()
	fmt.Println("Raft started")

	//----------------------START DHT----------------------

	//Recupere les lettre valide pour ce server
	file_letter, err := os.Open("FilesLIST/files_" + nodeIDStr + ".lst")
	if err != nil {
		panic(err)
	}
	defer file_letter.Close()

	Letters, err := GetFirstLine(file_letter)
	if err != nil {
		panic(err)
	}

	next_IP, err := ReadFileAndCreateMap("ServerLIST/server_" + nodeIDStr + ".lst")
	if err != nil {
		panic(err)
	}

	// Affichez les paramètres du server
	fmt.Println("DHT started")
	// println(nodeIDInt, Letters)
	// fmt.Println(next_IP)

	go StartTCP(nodeIDInt, next_IP, Letters)

	//On attend une entrée pour quitter
	var input string
	fmt.Scanln(&input)
	fmt.Println("--Ended--")
}
