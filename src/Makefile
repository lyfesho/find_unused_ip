CC = gcc
CCC = g++
CFLAGS = -Wall -g -fPIC
INC = -I./inc
#1INC	+= /usr/include/MESA
LDFLAGS = -L/usr/local/lib/ -L../lib/
LIBS = -lMESA_handle_logger -lMESA_prof_load -lrdkafka -lmaxminddb
OBJ = sip_extract.o cJSON.o
TARGET = sip_extract

.cpp.o:
	$(CCC) -c $(CFLAGS) $(INC) $<

all: $(TARGET) CP

$(TARGET): $(OBJ)
	$(CCC) -o $@ $(OBJ) $(LDFLAGS) $(LIBS) 

CP:
	mv $(TARGET) ../bin/sip_extract

clean:
	-rm -rf $(OBJ) $(TARGET)

.PHONY:clean
