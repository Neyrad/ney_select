#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <math.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <sys/prctl.h>
#include <errno.h>
#include <limits.h>
#include <string.h>

#define DEBUG

#define CLOSE(fd) 						\
		{								\
			if (fd != POISON)			\
			{							\
				close(fd);				\
				fd = POISON;			\
			}							\
		}

const int READ 			 = 0  ;
const int WRITE 		 = 1  ;
const int MAX_N_CHILDREN = 10 ;
const int MAX_MSG_SIZE   = 100;
const int POISON  		 = 666;
const int PARENT_ID		 = -1 ;

struct circleBuffer
{
	char* start;
	size_t size;
	int readOffset;
	int writeOffset;
	int isFull;
};

struct pipeData
{
	pid_t parentPid;
	pid_t childPid;

	int fd_C2P[2];
	int fd_P2C[2];

	struct circleBuffer buf;
};


int isFull (struct circleBuffer* buf);
int isEmpty(struct circleBuffer* buf);

int readToBuf   (struct pipeData* pipes, int id, int n);
int writeFromBuf(struct pipeData* pipes, int id, int n);

void freeBuffers(struct pipeData* pipes, int n);

void createParent(struct pipeData* pipes, int id, int n, int childPid);
void runParent(struct pipeData* pipes, int n);

void createChild(struct pipeData* pipes, int id, int n, int* fileFd, char* filepath);
void runChild(struct pipeData* pipes, int id, int fileFd);

int main(int argc, char* argv[])
{
	int n 	 = 0		;
	int myId = PARENT_ID;

	if (argc != 3)
	{
		fprintf(stderr, "Usage: %s [number of children] [filename]\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	char* endptr = NULL;
	n = strtoll(argv[1], &endptr, 10);

	if ((errno == ERANGE && (n == LLONG_MAX || n == LLONG_MIN)) || \
		(errno != 0 && n == 0) || n <= 0 || n > MAX_N_CHILDREN)
	{
		fprintf(stderr, "Wrong argument\n");
		exit(EXIT_FAILURE);
	}

	struct pipeData* pipes = (struct pipeData*) calloc(n, sizeof(struct pipeData));

	int fileFd = POISON;

	for (int i = 0; i < n; ++i)
	{
		pipe(pipes[i].fd_C2P);
		pipe(pipes[i].fd_P2C);
		pipes[i].parentPid = getpid();

		pid_t childPid = fork();
		if (childPid != 0)
			createParent(pipes, i, n, childPid);
		else
		{
			myId = i;
			createChild(pipes, i, n, &fileFd, argv[2]);
			break;
		}
	}

	if (myId == PARENT_ID)
		runParent(pipes, n);
	else
		runChild(pipes, myId, fileFd);
}

void createChild(struct pipeData* pipes, int id, int n, int* fileFd, char* filepath)
{
	if (id == 0)
	{
		*fileFd = open(filepath, O_RDONLY);
		if (*fileFd < 0)
		{
			fprintf(stderr, "Error while opening the file\n");
			freeBuffers(pipes, n);
			exit(EXIT_FAILURE);
		}
	}
	else
	{
		CLOSE(pipes[id-1].fd_P2C[WRITE]);
	}

	CLOSE(pipes[id].fd_C2P[READ]);

	CLOSE(pipes[id].fd_P2C[READ]);
	CLOSE(pipes[id].fd_P2C[WRITE]);

	for (int j = 0; j < id; ++j)
	{
		CLOSE(pipes[j].fd_P2C[WRITE]);
		CLOSE(pipes[j].fd_C2P[READ] );
	}

	prctl(PR_SET_PDEATHSIG, SIGKILL);
	if (getppid() != pipes[id].parentPid)
	{
		fprintf(stderr, "Parent has died: old ppid %d, new ppid %d\n", \
		pipes[id].parentPid, getppid());
		exit(EXIT_FAILURE);
	}
}

void runChild (struct pipeData* pipes, int id, int fileFd)
{
	int retVal = 0;

	while (1)
	{
		if (id == 0)
			retVal = splice(fileFd, NULL, pipes[id].fd_C2P[WRITE], \
									NULL, MAX_MSG_SIZE, SPLICE_F_MOVE);
		else
			retVal = splice (pipes[id-1].fd_P2C[READ],  NULL, \
							 pipes[id  ].fd_C2P[WRITE], NULL, \
							 MAX_MSG_SIZE, SPLICE_F_MOVE);

		if (retVal == 0)
		{
			CLOSE(pipes[id].fd_C2P[WRITE]);
			if (id == 0)
			{
				CLOSE(fileFd);
			}
			else
			{
				CLOSE(pipes[id-1].fd_P2C[READ]);
			}
			exit(EXIT_SUCCESS);
		}

		if (retVal < 0)
		{
			fprintf(stderr, "Splice has failed in a child #%d\n", id);
			perror("Splice");
			exit(EXIT_FAILURE);
		}
	}
}

void createParent(struct pipeData* pipes, int id, int n, int childPid)
{
	CLOSE(pipes[id].fd_C2P[WRITE]);
	if (id == n-1)
	{
		CLOSE(pipes[id].fd_P2C[READ]);
		CLOSE(pipes[id].fd_P2C[WRITE]);
		pipes[id].fd_P2C[WRITE] = STDOUT_FILENO;
	}
		pipes[id].childPid = childPid;
}

void runParent(struct pipeData* pipes, int n)
{
	for (int i = 0; i < n-1; ++i)
	{
		CLOSE(pipes[i].fd_P2C[READ]);
	}

	for (int i = 0; i < n; ++i)
	{
		struct pipeData* cur = &pipes[i];
		cur->buf.size 	  = pow (3, n - i + 4) * 1024;
		cur->buf.start = (char*) calloc(cur->buf.size, sizeof(char));

		if (!cur->buf.start)
		{
			fprintf(stderr, "Error allocating buf\n");
			freeBuffers(pipes, n);
			exit(EXIT_FAILURE);
		}

		fcntl(cur->fd_C2P[READ],  F_SETFL, O_NONBLOCK);
		fcntl(cur->fd_P2C[WRITE], F_SETFL, O_NONBLOCK);
	}

	fd_set  readFds = {0};
	fd_set writeFds = {0};

	int deadChildren = 0;

	while (deadChildren != n)
	{
		FD_ZERO (&readFds);
		FD_ZERO (&writeFds);
		int maxFd = 0;

		for (int i = deadChildren; i < n; ++i)
		{
			int readFd  = pipes[i].fd_C2P[READ];
			int writeFd = pipes[i].fd_P2C[WRITE];

			if (!isFull(&pipes[i].buf) && (readFd != POISON))
			{
				FD_SET(readFd, &readFds);
				maxFd = (readFd > maxFd ? readFd : maxFd);
			}

			if (!isEmpty(&pipes[i].buf) && (writeFd != POISON))
			{
				FD_SET(writeFd, &writeFds);
				maxFd = (writeFd > maxFd ? writeFd : maxFd);
			}
		}

		int retVal = select(maxFd + 1, &readFds, &writeFds, NULL, NULL);
		if (retVal < 0)
		{
			fprintf(stderr, "Select error\n");
			if (errno == EINTR)
			{
				fprintf(stderr, "Signal interrupt caught\n");
				continue;
			}
			freeBuffers(pipes, n);
			exit(EXIT_FAILURE);
		}

		if (retVal == 0)
			continue;

		for (int i = deadChildren; i < n; ++i)
		{
			if (FD_ISSET(pipes[i].fd_C2P[READ], &readFds))
			{
				int retVal = readToBuf(pipes, i, n);

				if (retVal == 0)
				{
					CLOSE(pipes[i].fd_C2P[READ]);
				}
			}

			if (FD_ISSET(pipes[i].fd_P2C[WRITE], &writeFds))
			{
				int retVal = writeFromBuf(pipes, i, n);
			}

			if (isEmpty(&pipes[i].buf) && (pipes[i].fd_C2P[READ] == POISON))
			{
				waitpid(pipes[i].childPid, NULL, 0);

				if (i != deadChildren++)
				{
					fprintf(stderr, "Wrong child death sequence\n");
					freeBuffers(pipes, n);
					exit(EXIT_FAILURE);
				}

				CLOSE(pipes[i].fd_P2C[WRITE]);
			}
		}
	}
}

void freeBuffers(struct pipeData* pipes, int n)
{
	for (int i = 0; i < n; ++i)
		free(pipes[i].buf.start);
}

int isFull(struct circleBuffer* buf)
{
	return buf->isFull;
}

int isEmpty(struct circleBuffer* buf)
{
	return !buf->isFull && (buf->readOffset == buf->writeOffset);
}

int readToBuf(struct pipeData* pipes, int id, int n)
{
	struct circleBuffer* cur = &pipes[id].buf;

	if (isFull(cur))
	{
		fprintf(stderr, "Attempt to read in a full buf in the connection %d\n", id);
		freeBuffers(pipes, n);
		exit(EXIT_FAILURE);
	}

	int retVal = -1;

	if (cur->writeOffset > cur->readOffset)
		retVal = read(pipes[id].fd_C2P[READ], cur->start + cur->readOffset,\
					  cur->writeOffset - cur->readOffset);
	else
		retVal = read(pipes[id].fd_C2P[READ], cur->start + cur->readOffset,\
					  cur->size - cur->readOffset);

	if (retVal < 0)
	{
		fprintf(stderr, "Error while reading to buf in the connection %d\n", id);
		freeBuffers(pipes, n);
		exit(EXIT_FAILURE);
	}

	cur->readOffset += retVal;
	cur->readOffset %= cur->size;

	if (retVal != 0)
		if (cur->readOffset == cur->writeOffset)
			cur->isFull = 1;

	return retVal;
}

int writeFromBuf(struct pipeData* pipes, int id, int n)
{
	struct circleBuffer* cur = &pipes[id].buf;
	int retVal = -1;

	if (isEmpty(cur))
	{
		fprintf(stderr, "Attempt to write from an empty buf in the connection %d\n", id);
		freeBuffers(pipes, n);
		exit(EXIT_FAILURE);
	}

	if (cur->writeOffset >= cur->readOffset)
	{
		if ((cur->writeOffset == cur->readOffset && isFull(cur)) || \
			 cur->writeOffset != cur->readOffset)
		{
			retVal = write(pipes[id].fd_P2C[WRITE], 				\
					 	   cur->start + cur->writeOffset,			\
						   cur->size	 - cur->writeOffset			);
		}
	}
	else
			retVal = write(pipes[id].fd_P2C[WRITE],					\
						   cur->start   + cur->writeOffset,	    	\
						   cur->readOffset - cur->writeOffset		);

	if (retVal < 0)
	{
		fprintf(stderr, "Error while writing from a buf in the connection %d\n", id);
		freeBuffers(pipes, n);
		exit(EXIT_FAILURE);
	}

	cur->writeOffset += retVal;
	cur->writeOffset %= cur->size;

	if (retVal != 0)
		cur->isFull = 0;

	return retVal;
}
