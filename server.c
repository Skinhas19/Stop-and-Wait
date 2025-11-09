#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>

// --- Definições do Protocolo ---

#define PORT 8888
#define MAX_BUFFER 1024
#define TIMEOUT_SEC 2 // Aumentei um pouco para redes mais lentas

#define PAYLOAD_SIZE (MAX_BUFFER - sizeof(int) - sizeof(int))

// Tipos de Operação
#define OP_UPLOAD 1
#define OP_DOWNLOAD 2

// Pacote de Requisição Inicial (cliente -> servidor)
typedef struct {
    int operation; // OP_UPLOAD ou OP_DOWNLOAD
    char filename[256];
} RequestPacket;

// Pacote de Dados
typedef struct {
    int seq_num;
    int data_len;
    char data[PAYLOAD_SIZE];
} DataPacket;

// Pacote de Confirmação (ACK)
typedef struct {
    int ack_num;
} AckPacket;

// Função de utilidade para printar erros
void die(char *s) {
    perror(s);
    exit(1);
}

// Estrutura para passar dados para a thread
typedef struct {
    struct sockaddr_in client_addr;
    socklen_t client_len;
    RequestPacket request;
} ThreadArgs;


// --- Lógica de Envio (Stop-and-Wait Sender) ---
void send_file(int sock, const char* filename, struct sockaddr_in client_addr, socklen_t client_len) {
    printf("[Thread %ld] Iniciando envio de '%s'\n", pthread_self(), filename);

    FILE *fp = fopen(filename, "rb");
    if (fp == NULL) {
        printf("[Thread %ld] Erro: Arquivo '%s' não encontrado.\n", pthread_self(), filename);
        // Envia um pacote "FIN" com erro (data_len < 0) para notificar o cliente
        DataPacket err_packet;
        err_packet.seq_num = 0;
        err_packet.data_len = -1; // Sinal de erro
        sendto(sock, &err_packet, sizeof(DataPacket), 0, (struct sockaddr *)&client_addr, client_len);
        return;
    }
    
    // Configura timeout no socket da thread
    struct timeval tv;
    tv.tv_sec = TIMEOUT_SEC;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    DataPacket packet;
    AckPacket ack;
    int current_seq = 0;
    size_t bytes_read;

    while ((bytes_read = fread(packet.data, 1, PAYLOAD_SIZE, fp)) > 0) {
        packet.seq_num = current_seq;
        packet.data_len = bytes_read;
        int ack_received = 0;

        while (!ack_received) {
            printf("[Thread %ld] Enviando seq=%d (%d bytes)...\n", pthread_self(), packet.seq_num, packet.data_len);
            sendto(sock, &packet, sizeof(DataPacket), 0, (struct sockaddr *)&client_addr, client_len);
            
            if (recvfrom(sock, &ack, sizeof(AckPacket), 0, (struct sockaddr *)&client_addr, &client_len) > 0) {
                if (ack.ack_num == current_seq) {
                    ack_received = 1;
                    current_seq = 1 - current_seq;
                }
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                printf("[Thread %ld] TIMEOUT! Reenviando seq=%d...\n", pthread_self(), packet.seq_num);
            } else {
                die("recvfrom_sender");
            }
        }
    }

    // Envio do pacote FIN
    packet.seq_num = current_seq;
    packet.data_len = 0;
    int ack_received = 0;
    while(!ack_received) {
         sendto(sock, &packet, sizeof(DataPacket), 0, (struct sockaddr *)&client_addr, client_len);
         if (recvfrom(sock, &ack, sizeof(AckPacket), 0, (struct sockaddr *)&client_addr, &client_len) > 0 && ack.ack_num == current_seq) {
             ack_received = 1;
         }
    }
    printf("[Thread %ld] Envio de '%s' concluído.\n", pthread_self(), filename);
    fclose(fp);
}

// --- Lógica de Recebimento (Stop-and-Wait Receiver) ---
void receive_file(int sock, const char* filename, struct sockaddr_in client_addr, socklen_t client_len) {
    printf("[Thread %ld] Iniciando recepção para salvar como '%s'\n", pthread_self(), filename);
    
    FILE *fp = fopen(filename, "wb");
    if (fp == NULL) die("fopen");

    DataPacket packet;
    AckPacket ack;
    int expected_seq = 0;

    while (1) {
        if (recvfrom(sock, &packet, sizeof(DataPacket), 0, (struct sockaddr *)&client_addr, &client_len) > 0) {
            if (packet.data_len == 0 && packet.seq_num == expected_seq) {
                ack.ack_num = expected_seq;
                sendto(sock, &ack, sizeof(AckPacket), 0, (struct sockaddr *)&client_addr, client_len);
                break; // Fim da transferência
            }

            if (packet.seq_num == expected_seq) {
                fwrite(packet.data, 1, packet.data_len, fp);
                ack.ack_num = expected_seq;
                expected_seq = 1 - expected_seq;
            } else {
                ack.ack_num = 1 - expected_seq; // Reenviar ACK anterior
            }
            sendto(sock, &ack, sizeof(AckPacket), 0, (struct sockaddr *)&client_addr, client_len);
        }
    }

    printf("[Thread %ld] Recepção de '%s' concluída.\n", pthread_self(), filename);
    fclose(fp);
}

// Função principal da Thread
void *handle_client(void *args) {
    ThreadArgs *t_args = (ThreadArgs *)args;
    
    int thread_sock;
    if ((thread_sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1) {
        die("thread socket");
    }

    if (t_args->request.operation == OP_UPLOAD) {
        // Cliente quer enviar, então o servidor recebe
        // Primeiro, enviamos um ACK para a requisição inicial para o cliente começar a enviar os dados
        AckPacket req_ack = { .ack_num = 0 };
        sendto(thread_sock, &req_ack, sizeof(AckPacket), 0, (struct sockaddr *) &t_args->client_addr, t_args->client_len);

        receive_file(thread_sock, t_args->request.filename, t_args->client_addr, t_args->client_len);

    } else if (t_args->request.operation == OP_DOWNLOAD) {
        // Cliente quer baixar, então o servidor envia
        send_file(thread_sock, t_args->request.filename, t_args->client_addr, t_args->client_len);
    }

    close(thread_sock);
    free(t_args);
    pthread_exit(NULL);
}

int main(void) {
    struct sockaddr_in si_me, si_other;
    int s;
    socklen_t slen = sizeof(si_other);

    if ((s = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1) die("socket");

    memset((char *)&si_me, 0, sizeof(si_me));
    si_me.sin_family = AF_INET;
    si_me.sin_port = htons(PORT);
    si_me.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(s, (struct sockaddr *)&si_me, sizeof(si_me)) == -1) die("bind");

    printf("Servidor UDP ouvindo na porta %d...\n", PORT);

    while (1) {
        ThreadArgs *args = (ThreadArgs *)malloc(sizeof(ThreadArgs));
        if (args == NULL) die("malloc");
        
        args->client_len = slen;

        printf("Aguardando nova requisição de cliente...\n");
        if (recvfrom(s, &(args->request), sizeof(RequestPacket), 0, 
                    (struct sockaddr *)&(args->client_addr), &(args->client_len)) > 0) {
            
            printf("Recebida requisição de %s:%d. Operação: %s, Arquivo: %s. Criando thread...\n",
                   inet_ntoa(args->client_addr.sin_addr), ntohs(args->client_addr.sin_port),
                   (args->request.operation == OP_UPLOAD ? "UPLOAD" : "DOWNLOAD"),
                   args->request.filename);

            pthread_t tid;
            if (pthread_create(&tid, NULL, handle_client, (void *)args) != 0) {
                die("pthread_create");
            }
            pthread_detach(tid); 
        }
    }
    close(s);
    return 0;
}