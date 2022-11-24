package mpi

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

type MPIWorld struct {
	size   uint64
	rank   []uint64
	IPPool []string
	Port   []uint64
}

func SerializeWorld(world *MPIWorld) []byte {
	// serialize the MPIWorld struct
	// format: size, rank, IPPool, Port
	// size: uint64
	buf := make([]byte, 0)
	sizebuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(sizebuf, world.size)
	buf = append(buf, sizebuf...)

	// rank: []uint64
	for _, rank := range world.rank {
		rankBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(rankBuf, rank)
		buf = append(buf, rankBuf...)
	}

	// IPPool: []string
	for _, ip := range world.IPPool {
		IPBuf := make([]byte, 0)
		IPBuf = append(IPBuf, []byte(ip)...)
		IPBuf = append(IPBuf, 0)
		buf = append(buf, IPBuf...)
	}

	// Port: []uint64
	for _, port := range world.Port {
		portBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(portBuf, port)
		buf = append(buf, portBuf...)
	}
	return buf
}

func DeserializeWorld(buf []byte) *MPIWorld {
	// deserialize the MPIWorld struct
	// format: size, rank, IPPool, Port
	// size: uint64
	world := new(MPIWorld)
	world.size = binary.LittleEndian.Uint64(buf[:8])
	buf = buf[8:]

	// rank: []uint64
	world.rank = make([]uint64, world.size)
	for i := uint64(0); i < world.size; i++ {
		world.rank[i] = binary.LittleEndian.Uint64(buf[:8])
		buf = buf[8:]
	}

	// IPPool: []string
	world.IPPool = make([]string, world.size)
	for i := uint64(0); i < world.size; i++ {
		end := 0
		for end < len(buf) && buf[end] != 0 {
			end++
		}
		world.IPPool[i] = string(buf[:end])
		buf = buf[end+1:]
	}

	// Port: []uint64
	world.Port = make([]uint64, world.size)
	for i := uint64(0); i < world.size; i++ {
		world.Port[i] = binary.LittleEndian.Uint64(buf[:8])
		buf = buf[8:]
	}
	return world
}

var (
	SelfRank              uint64
	MasterToSlaveTCPConn  []*net.Conn
	SlaveToMasterTCPConn  *net.Conn
	MasterToSlaveListener []*net.Listener
	SlaveOutputs          []bytes.Buffer
	SlaveOutputsErr       []bytes.Buffer
	BytesSent             uint64
	BytesReceived         uint64
	WorldSize             uint64
)

func SetIPPool(filePath string, world *MPIWorld) error {
	// reading IP from file, the first IP is the master node
	// the rest are the slave nodes
	ipFile, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer ipFile.Close()
	scanner := bufio.NewScanner(ipFile)
	for scanner.Scan() {
		line := scanner.Text()
		//IP and hostname are separated by a space

		if strings.HasPrefix(line, "#") {
			continue
		}
		if len(line) == 0 {
			continue
		}

		world.IPPool = append(world.IPPool, strings.Split(line, " ")[0])
		hostname := strings.Split(line, " ")[1]

		//if hostname doesn't start with "node" we skip
		if !(hostname[:4] == "node" || (len(hostname) >= 6 && hostname[:6] == "master")) {
			continue
		}
		if err != nil {
			return err
		}
		// get a random port number betwee 10000 and 20000
		port := uint64(10000 + rand.Intn(10000))
		world.Port = append(world.Port, uint64(port))
		world.rank = append(world.rank, world.size)
		world.size++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func GetLocalIP() ([]string, error) {
	// get local IP address
	addrs, err := net.InterfaceAddrs()
	result := make([]string, 0)
	if err != nil {
		return result, err
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok {
			if ipnet.IP.To4() != nil {
				result = append(result, ipnet.IP.String())
			}
		}
	}
	return result, nil
}

func checkSlave() bool {
	LastCommand := os.Args[len(os.Args)-1]
	return strings.ToLower(LastCommand) == "slave"
}

type config struct {
	User    string
	KeyFile string
	Verbose bool
}

func ParseConfig(ConfigFilePath string) config {
	// parse the config file
	// format: user, keyfile, verbose
	// user: string
	// keyfile: string
	// verbose: bool
	config := config{}
	configFile, err := os.Open(ConfigFilePath)
	if err != nil {
		panic(err)
	}
	defer configFile.Close()
	scanner := bufio.NewScanner(configFile)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "user") {
			config.User = strings.Split(line, " ")[1]
		} else if strings.HasPrefix(line, "keyfile") {
			config.KeyFile = strings.Split(line, " ")[1]
		} else if strings.HasPrefix(line, "verbose") {
			config.Verbose, err = strconv.ParseBool(strings.Split(line, " ")[1])
			if err != nil {
				panic(err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	return config
}

func WorldInit(HostFilePath string, ConfigFilePath string) *MPIWorld {
	world := new(MPIWorld)
	world.size = 0
	world.rank = make([]uint64, 0)
	world.IPPool = make([]string, 0)
	world.Port = make([]uint64, 0)

	selfIP, _ := GetLocalIP()
	fmt.Println(selfIP)

	isSlave := checkSlave()

	configuration := ParseConfig(ConfigFilePath)

	//Setup TCP connections master <--> slaves

	if !isSlave {
		err := SetIPPool(HostFilePath, world)
		if err != nil {
			fmt.Println(err)
			panic(err)
		}
		MasterToSlaveTCPConn = make([]*net.Conn, world.size)
		SlaveOutputs = make([]bytes.Buffer, world.size)
		SlaveOutputsErr = make([]bytes.Buffer, world.size)
		MasterToSlaveListener = make([]*net.Listener, world.size)
		MasterToSlaveTCPConn[0] = nil
		selfFileLocation, _ := os.Executable()
		SelfRank = 0
		for i := 1; i < int(world.size); i++ {
			slaveIP := world.IPPool[i]
			slavePort := world.Port[i]
			slaveRank := uint64(i)

			// Start slave process via ssh
			fmt.Println(configuration.KeyFile)
			key, err := ioutil.ReadFile(configuration.KeyFile)
			if err != nil {
				fmt.Printf("unable to read private key: %v\n", err)
				panic("Failed to load key")
			}
			signer, err := ssh.ParsePrivateKey(key)
			if err != nil {
				fmt.Printf("unable to parse private key: %v\n", err)
				panic("Failed to parse key")
			}
			fmt.Println(slaveIP)
			conn, err := ssh.Dial("tcp", slaveIP+":"+strconv.Itoa(int(22)), &ssh.ClientConfig{
				User: configuration.User,
				Auth: []ssh.AuthMethod{
					ssh.PublicKeys(signer),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			})

			if err != nil {
				fmt.Println(err)
				panic("Failed to dial: " + err.Error())
			}

			session, err := conn.NewSession()
			if err != nil {
				fmt.Println(err)
				panic("Failed to create session: " + err.Error())
			}
			Command := selfFileLocation
			for j := 1; j < len(os.Args); j++ {
				Command += " " + os.Args[j]
			}
			Command += " " + world.IPPool[0] + " " + strconv.Itoa(int(world.Port[i]))
			Command += " Slave"

			//run the command async and panic when command return error
			go func() {
				defer session.Close()
				session.Stdout = &SlaveOutputs[i]
				session.Stderr = &SlaveOutputsErr[i]
				err := session.Run(Command)

				if err != nil {
					fmt.Println(err)
				}
			}()

			go func(rank uint64) {
				// Print the output of the command
				for {
					data, _ := SlaveOutputs[rank].ReadString('\n')
					if data != "" && configuration.Verbose {
						fmt.Println("rank " + strconv.Itoa(int(rank)) + " " + data)
					}
					data, _ = SlaveOutputsErr[rank].ReadString('\n')
					if data != "" {
						ErrorColor := "\033[1;31m%s\033[0m"
						fmt.Printf(ErrorColor, "rank "+strconv.Itoa(int(rank))+" ERR "+data)
					}
					time.Sleep(1 * time.Microsecond)
				}
			}(uint64(i))

			// Listen to slave
			listener, err := net.Listen("tcp", ":"+strconv.Itoa(int(slavePort)))
			if err != nil {
				fmt.Println(err)
				panic("Failed to listen: " + err.Error())
			}
			// Accept a connection
			TCPConn, err := listener.Accept()

			MasterToSlaveTCPConn[i] = &TCPConn
			MasterToSlaveListener[i] = &listener
			if err != nil {
				fmt.Println(err)
				panic("Failed to connect via TCP: " + err.Error())
			}
			fmt.Println("Connected to slave " + strconv.Itoa(i))

			// Send slave rank
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(slaveRank))
			_, err = TCPConn.Write(buf)
			if err != nil {
				fmt.Println(err)
				panic("Failed to send rank: " + err.Error())
			}

			// Send the working directory
			{
				workingDir, err := os.Getwd()
				if err != nil {
					fmt.Println(err)
					panic("Failed to get working directory: " + err.Error())
				}
				//Send string length
				buf = make([]byte, 8)
				binary.LittleEndian.PutUint64(buf, uint64(len(workingDir)))
				_, err = TCPConn.Write(buf)
				if err != nil {
					fmt.Println(err)
					panic("Failed to send working directory length: " + err.Error())
				}
				//Send string
				_, err = TCPConn.Write([]byte(workingDir))
				if err != nil {
					fmt.Println(err)
					panic("Failed to send working directory: " + err.Error())
				}
				fmt.Println("Sent working directory to slave " + strconv.Itoa(i))
			}

			// Sync the world state
			buf = SerializeWorld(world)

			//Send buf size
			bufSize := make([]byte, 8)
			binary.LittleEndian.PutUint64(bufSize, uint64(len(buf)))
			_, err = TCPConn.Write(bufSize)
			if err != nil {
				fmt.Println(err)
				panic("Failed to send buf size: " + err.Error())
			}

			//Send buf
			_, err = TCPConn.Write(buf)
			if err != nil {
				fmt.Println(err)
				panic("Failed to send world: " + err.Error())
			}

		}
	} else {
		// connect to master
		masterIP := os.Args[len(os.Args)-3]
		slavePort := os.Args[len(os.Args)-2]
		TCPConn, err := net.Dial("tcp", masterIP+":"+slavePort)
		SlaveToMasterTCPConn = &TCPConn
		if err != nil {
			fmt.Println(err)
			panic("Failed to accept: " + err.Error())
		}
		// Receive master rank
		buf := make([]byte, 8)
		_, err = TCPConn.Read(buf)
		if err != nil {
			fmt.Println(err)
			panic("Failed to receive rank: " + err.Error())
		}
		SelfRank = binary.LittleEndian.Uint64(buf)

		// Receive the working directory
		{
			//Receive string length
			buf = make([]byte, 8)
			_, err = TCPConn.Read(buf)
			if err != nil {
				fmt.Println(err)
				panic("Failed to receive working directory length: " + err.Error())
			}
			workingDirLength := binary.LittleEndian.Uint64(buf)
			//Receive string
			buf = make([]byte, workingDirLength)
			_, err = TCPConn.Read(buf)
			if err != nil {
				fmt.Println(err)
				panic("Failed to receive working directory: " + err.Error())
			}
			workingDir := string(buf)
			err = os.Chdir(workingDir)
			if err != nil {
				fmt.Println(err)
				panic("Failed to change working directory: " + err.Error())
			}
			workingDir, _ = os.Getwd()
			fmt.Println("Changed working directory to " + workingDir)
		}

		// Sync the world state
		// Receive buf size
		bufSize := make([]byte, 8)
		_, err = TCPConn.Read(bufSize)
		if err != nil {
			fmt.Println(err)
			panic("Failed to receive buf size: " + err.Error())
		}
		buf = make([]byte, binary.LittleEndian.Uint64(bufSize))
		fmt.Println("Received buf size " + strconv.Itoa(int(binary.LittleEndian.Uint64(bufSize))))

		_, err = TCPConn.Read(buf)
		if err != nil {
			fmt.Println(err)
			panic("Failed to receive world: " + err.Error())
		}
		world = DeserializeWorld(buf)
	}
	WorldSize = world.size
	return world
}

// If Master calls this function, rank is required
// If Slave calls this function, rank is not required, it will send to Master
var sentBytes []byte
var recvBytes []byte

func SendBytes(buf []byte, rank uint64) error {
	var errorMsg error
	errorMsg = nil
	BytesSentInThisSession := 0
	sentBytes = append(sentBytes, buf...)
	for len(buf) > 0 {
		n := 0
		if SelfRank == 0 {
			n, errorMsg = (*MasterToSlaveTCPConn[rank]).Write(buf)
		} else {
			n, errorMsg = (*SlaveToMasterTCPConn).Write(buf)
		}
		if errorMsg != nil {
			fmt.Println(string(debug.Stack()))
			return errorMsg
		}
		BytesSentInThisSession += n
		BytesSent += uint64(n)
		buf = buf[n:]
	}
	return errorMsg
}

// If Master calls this function, rank is required, it will receive from rank-th slave
// If Slave calls this function, rank is not required, it will receive from Master
func ReceiveBytes(size uint64, rank uint64) ([]byte, error) {
	buf := make([]byte, size)
	var errorMsg error
	errorMsg = nil
	BytesRead := uint64(0)
	for BytesRead < size {
		n := 0
		tmpBuf := make([]byte, size-BytesRead)
		if SelfRank == 0 {
			(*MasterToSlaveTCPConn[rank]).SetReadDeadline(time.Now().Add(10 * time.Second))
			n, errorMsg = (*MasterToSlaveTCPConn[rank]).Read(tmpBuf)
		} else {
			(*SlaveToMasterTCPConn).SetReadDeadline(time.Now().Add(10 * time.Second))
			n, errorMsg = (*SlaveToMasterTCPConn).Read(tmpBuf)
		}
		for i := BytesRead; i < BytesRead+uint64(n); i++ {
			buf[i] = tmpBuf[i-BytesRead]
		}
		if errorMsg != nil {
			if errorMsg.Error() == "EOF" {
				fmt.Println("EOF")
			}
			fmt.Println(string(debug.Stack()))
			return buf, errorMsg
		}
		BytesReceived += uint64(n)
		BytesRead += uint64(n)
	}
	recvBytes = append(recvBytes, buf...)
	return buf, errorMsg
}

func GetHash(str string) {
	fmt.Println(str + " Bytes sent: " + strconv.Itoa(int(BytesSent)))
	fmt.Println(str + " Bytes received: " + strconv.Itoa(int(BytesReceived)))
	fmt.Println(str + " Sent hash: " + fmt.Sprintf("%x", md5.Sum(sentBytes)))
	fmt.Println(str + " Received hash: " + fmt.Sprintf("%x", md5.Sum(recvBytes)))
}

func Close() {
	fmt.Println("Bytes sent: " + strconv.Itoa(int(BytesSent)))
	fmt.Println("Bytes received: " + strconv.Itoa(int(BytesReceived)))
	fmt.Println("Sent hash: " + fmt.Sprintf("%x", md5.Sum(sentBytes)))
	fmt.Println("Received hash: " + fmt.Sprintf("%x", md5.Sum(recvBytes)))
	if SelfRank == 0 {
		time.Sleep(1 * time.Second)
		for i := 1; i < len(MasterToSlaveTCPConn); i++ {
			(*MasterToSlaveTCPConn[i]).Close()
			(*MasterToSlaveListener[i]).Close()
		}
	} else {
		(*SlaveToMasterTCPConn).Close()
	}
}
