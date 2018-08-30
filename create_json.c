#include <stdio.h>
#include <string.h>

int main() {
	FILE *ptr_readfile;
	FILE *ptr_writefile;
	char line [40000]; /* or some other suitable maximum line size */
	char fileoutputname[15];
	int filecounter=1, linecounter=1;

	ptr_readfile = fopen("/run/media/gabriel/LinuxExtra/trabalho-spark/spark_reddit/RC_2016-06.json","r");
	if (!ptr_readfile)
		return 1;

	sprintf(fileoutputname, "/run/media/gabriel/LinuxExtra/trabalho-spark/spark_reddit/reddit_posts_2016/file_part%d.json", filecounter);
	ptr_writefile = fopen(fileoutputname, "w");

	while (fgets(line, sizeof line, ptr_readfile)!=NULL) {
		if (linecounter == 30) {
			fclose(ptr_writefile);
			linecounter = 1;
			filecounter++;
			if(filecounter >= 100000)
				break;

			sprintf(fileoutputname, "/run/media/gabriel/LinuxExtra/trabalho-spark/spark_reddit/reddit_posts_2016/file_part%d.json", filecounter);
			
			ptr_writefile = fopen(fileoutputname, "w");
			if (!ptr_writefile)
				return 1;
			memset(line, 0, sizeof(line));
		}
		fprintf(ptr_writefile,"%s", line);
		linecounter++;
	}
	fclose(ptr_readfile);
	return 0;
}
