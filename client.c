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
#define TIMEOUT_SEC 2

#define PAYLOAD_SIZE (MAX_BUFFER - sizeof(int) - sizeof(int))

// Tipos de Operação
#define OP_UPLOAD 1
#define OP_DOWNLOAD 2

// Pacote de Requisição Inicial (cliente -> servidor)
typedef struct {
    int operation;
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

void die(char *s) {
    perror(s);
    exit(1);
}

// --- Lógica de Envio (Stop-and-Wait Sender) ---
void perform_upload(int sock, const char* filename, struct sockaddr_in si_other, socklen_t slen) {
    FILE *fp = fopen(filename, "rb");
    if (fp == NULL) die("fopen");

    DataPacket packet;
    AckPacket ack;
    int current_seq = 0;
    size_t bytes_read;

    while ((bytes_read = fread(packet.data, 1, PAYLOAD_SIZE, fp)) > 0) {
        packet.seq_num = current_seq;
        packet.data_len = bytes_read;
        int ack_received = 0;

        while (!ack_received) {
            printf("Enviando seq=%d (%zu bytes)...\n", packet.seq_num, bytes_read);
            sendto(sock, &packet, sizeof(DataPacket), 0, (struct sockaddr *)&si_other, slen);
            
            if (recvfrom(sock, &ack, sizeof(AckPacket), 0, (struct sockaddr *)&si_other, &slen) > 0) {
                if (ack.ack_num == current_seq) {
                    printf("Recebeu ACK=%d. Sucesso.\n", ack.ack_num);
                    ack_received = 1;
                    current_seq = 1 - current_seq;
                }
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                printf("TIMEOUT! Reenviando seq=%d...\n", packet.seq_num);
            } else {
                die("recvfrom_upload");
            }
        }
    }

    packet.seq_num = current_seq;
    packet.data_len = 0; // Pacote FIN
    int ack_received = 0;
    while(!ack_received) {
        printf("Enviando pacote FIN (seq=%d)...\n", current_seq);
        sendto(sock, &packet, sizeof(DataPacket), 0, (struct sockaddr *)&si_other, slen);
        if (recvfrom(sock, &ack, sizeof(AckPacket), 0, (struct sockaddr *)&si_other, &slen) > 0 && ack.ack_num == current_seq) {
             ack_received = 1;
        }
    }
    printf("Upload de '%s' concluído.\n", filename);
    fclose(fp);
}

// --- Lógica de Recebimento (Stop-and-Wait Receiver) ---
void perform_download(int sock, const char* filename, struct sockaddr_in si_other, socklen_t slen) {
    char local_filename[260];
    snprintf(local_filename, sizeof(local_filename), "%s", filename);

    FILE *fp = fopen(local_filename, "wb");
    if (fp == NULL) die("fopen");

    DataPacket packet;
    AckPacket ack;
    int expected_seq = 0;

    printf("Aguardando dados para salvar como '%s'...\n", local_filename);

    while (1) {
        if (recvfrom(sock, &packet, sizeof(DataPacket), 0, (struct sockaddr *)&si_other, &slen) > 0) {
            // Checagem de erro do servidor (arquivo não encontrado)
            if (packet.data_len < 0) {
                printf("Erro do servidor: Arquivo não pôde ser aberto para download.\n");
                fclose(fp);
                remove(local_filename); // Remove o arquivo vazio
                return;
            }
            
            if (packet.data_len == 0 && packet.seq_num == expected_seq) {
                ack.ack_num = expected_seq;
                sendto(sock, &ack, sizeof(AckPacket), 0, (struct sockaddr *)&si_other, slen);
                break; // Fim da transferência
            }
            
            if (packet.seq_num == expected_seq) {
                printf("Recebeu seq=%d. Enviando ACK=%d\n", packet.seq_num, packet.seq_num);
                fwrite(packet.data, 1, packet.data_len, fp);
                ack.ack_num = expected_seq;
                expected_seq = 1 - expected_seq;
            } else {
                printf("Recebeu seq=%d (duplicado). Reenviando ACK=%d\n", packet.seq_num, (1-expected_seq));
                ack.ack_num = 1 - expected_seq;
            }
            sendto(sock, &ack, sizeof(AckPacket), 0, (struct sockaddr *)&si_other, slen);
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            printf("TIMEOUT esperando pacote do servidor. A conexão pode ter sido perdida.\n");
            break;
        }
    }

    printf("Download de '%s' concluído.\n", local_filename);
    fclose(fp);
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Uso: %s <upload|download> <nome_do_arquivo>\n", argv[0]);
        exit(1);
    }
    
    char* operation_str = argv[1];
    char* filename = argv[2];
    char* server_ip = "127.0.0.1";

    struct sockaddr_in si_other;
    int s;
    socklen_t slen = sizeof(si_other);
    
    if ((s = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1) die("socket");

    memset((char *)&si_other, 0, sizeof(si_other));
    si_other.sin_family = AF_INET;
    si_other.sin_port = htons(PORT);
    if (inet_aton(server_ip, &si_other.sin_addr) == 0) die("inet_aton");

    // Prepara e envia o pacote de requisição
    RequestPacket req;
    strncpy(req.filename, filename, sizeof(req.filename) - 1);
    req.filename[sizeof(req.filename) - 1] = '\0'; // Garante terminação nula

    if (strcmp(operation_str, "upload") == 0) {
        req.operation = OP_UPLOAD;
    } else if (strcmp(operation_str, "download") == 0) {
        req.operation = OP_DOWNLOAD;
    } else {
        fprintf(stderr, "Operação inválida: use 'upload' ou 'download'\n");
        exit(1);
    }

    // Envia a requisição inicial
    sendto(s, &req, sizeof(RequestPacket), 0, (struct sockaddr *)&si_other, slen);
    printf("Requisição de %s para o arquivo '%s' enviada ao servidor.\n", operation_str, filename);

    // Configura o timeout para a comunicação principal
    struct timeval tv;
    tv.tv_sec = TIMEOUT_SEC;
    tv.tv_usec = 0;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // Executa a operação
    if (req.operation == OP_UPLOAD) {
        // Espera o ACK da requisição para começar o envio dos dados
        AckPacket req_ack;
        if (recvfrom(s, &req_ack, sizeof(AckPacket), 0, (struct sockaddr*)&si_other, &slen) > 0) {
            if (req_ack.ack_num == 0) {
                printf("Servidor confirmou requisição de upload. Iniciando transferência...\n");
                perform_upload(s, filename, si_other, slen);
            }
        } else {
            printf("Servidor não respondeu à requisição de upload.\n");
        }
    } else if (req.operation == OP_DOWNLOAD) {
        perform_download(s, filename, si_other, slen);
    }

    close(s);
    return 0;
}