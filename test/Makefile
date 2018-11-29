CC = gcc
CCC = g++
CFLAGS = -Wall -g -fPIC
INC = -I./inc/
#1INC	+= /usr/include/MESA
LDFLAGS = -L../test/lib/
LIBS = -lMESA_handle_logger -lMESA_prof_load -lrdkafka
OBJ = producer_test.o cJSON.o
TARGET = rk_producer

.cpp.o:
	$(CCC) -c $(CFLAGS) $(INC) $<

all: $(TARGET) CP

$(TARGET): $(OBJ)
	$(CCC) -o $@ $(OBJ) $(LDFLAGS) $(LIBS) 

CP:
	mv $(TARGET) ../bin/producer_test

clean:
	-rm -rf $(OBJ) $(TARGET)

.PHONY:clean
