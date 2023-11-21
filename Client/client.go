package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

//----------------------------------------------------------------------
//----------------------------------WEB---------------------------------
//----------------------------------------------------------------------

//----------------------------------------------------------------------
//---------------------------------RAFT---------------------------------
//----------------------------------------------------------------------

//----------------------------------------------------------------------
//---------------------------------DHT----------------------------------
//----------------------------------------------------------------------

func SaveFile(content string, filename string) {

	file, err := os.Create("Result/" + filename)
	if err != nil {
		fmt.Printf("Erreur lors de la création du fichier : %v\n", err)
		return
	}
	defer file.Close()

	_, err = file.WriteString(content)
	if err != nil {
		fmt.Printf("Erreur lors de l'écriture dans le fichier : %v\n", err)
		return
	}

	fmt.Println("Fichier créé avec succès")
}

func SendFileRequest(Client_IP string, File string, FirstNode string) {
	message := Client_IP + "|" + File + "|" + "4" + "|" + "1"
	conn, err := net.Dial("tcp", FirstNode)
	if err != nil {
		fmt.Printf("Erreur lors de la connexion au serveur : %v\n", err)
		return
	}
	defer conn.Close()
	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Printf("Erreur lors de l'envoi du message : %v\n", err)
		return
	}
}

func handleConnection(conn net.Conn) {

	defer conn.Close()
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Printf("Erreur lors de la lecture depuis la connexion : %v\n", err)
			return
		}
		response := string(buffer[:n])
		if strings.Split(response, "|")[0] == "Found" {
			node := strings.Split(response, "|")[2]
			file := strings.Split(response, "|")[1]
			fmt.Printf("Le fichier a été trouvé au niveau %s\n", node)
			SaveFile(strings.Split(response, "|")[3], file)
			return

		} else if strings.HasPrefix(response, "FileList|") {
			filelist := strings.Split(response, "|")[1]
			files := strings.Split(filelist, ",")
			for _, file := range files {
				fmt.Println(file)
			}
		} else {
			fmt.Printf("Received : " + response + "\n")
			return
		}
	}
}

func StartTCPServer(Client_IP string) {

	listener, err := net.Listen("tcp", Client_IP)
	if err != nil {
		fmt.Printf("Erreur lors de la création de l'écouteur : %v\n", err)
		return
	}
	defer listener.Close()
	fmt.Println("Serveur TCP en cours d'écoute sur " + Client_IP + "\n")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Erreur lors de l'acceptation de la connexion : %v\n", err)
			continue
		}
		go handleConnection(conn)
	}
}

func main() {

	FirstNodeDht := "127.0.0.1:1000"
	FirstNodeRaft := "127.0.0.1:8002"
	Client_IP := "127.0.0.1:300"

	go StartTCPServer(Client_IP)

	time.Sleep(1 * time.Second)

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("Commandes disponibles :")
	fmt.Println("ADD <filename>")
	fmt.Println("DEL <filename>")
	fmt.Println("GET <filename>")
	fmt.Println("GETALL")

	for scanner.Scan() {
		input := scanner.Text()
		if input == "q" {
			break
		}

		commands := strings.Fields(input)
		if len(commands) < 2 && commands[0] != "GETALL" {
			fmt.Println("Commande invalide:", input)
			continue
		}

		switch commands[0] {
		case "ADD":
			file := commands[1]
			fmt.Println("Adding file:", file)
			_, err := http.Get("http://" + FirstNodeRaft + "/cmd?" + "cmdtype=add&message=" + file + "&port=300" + "&term=0")
			if err != nil {
				log.Println(err)
				return
			}

		case "DEL":
			file := commands[1]
			fmt.Println("Deleting file:", file)
			_, err := http.Get("http://" + FirstNodeRaft + "/cmd?" + "cmdtype=del&message=" + file + "&port=300" + "&term=0")
			if err != nil {
				log.Println(err)
				return
			}

		case "GET":
			file := commands[1]
			fmt.Println("Requesting file:", file)
			SendFileRequest(Client_IP, file, FirstNodeDht)

		case "GETALL":
			fmt.Println("Requesting all files")
			_, err := http.Get("http://" + FirstNodeRaft + "/getall?" + "&port=300" + "&term=0")
			if err != nil {
				log.Println(err)
				return
			}

		default:
			fmt.Println("Commande inconnue:", commands[0])
		}
	}

	fmt.Println("done")
}
