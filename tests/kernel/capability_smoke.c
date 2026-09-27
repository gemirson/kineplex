// SPDX-License-Identifier: GPL-2.0
/* FT-096: this process must receive EPERM before device access. */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

int main(int argc, char **argv)
{
	const char *path = argc > 1 ? argv[1] : "/dev/kinegeo";
	int fd = open(path, O_RDWR | O_CLOEXEC);

	if (fd >= 0) {
		close(fd);
		fprintf(stderr, "security failure: open unexpectedly succeeded\n");
		return 1;
	}
	if (errno != EPERM) {
		fprintf(stderr, "security failure: expected EPERM, got %s\n",
			strerror(errno));
		return 1;
	}
	printf("PASS: unprivileged open returned EPERM\n");
	return 0;
}
