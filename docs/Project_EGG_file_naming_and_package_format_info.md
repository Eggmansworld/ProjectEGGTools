## Downloading

User-Agent: c384da2W9f73dz20403d

All files are in the format (P|)XXXYZZZa.bin

"X" is the company code, one of the following:
```
 'AGL', 'ARS', 'ART', 'ATN', 'BGT', 'BOT', 'BPS', 'CAR', 'COM', 'COS', 'DBS', 'DMB', 'EBS', 'FAL', 'FAM', 'FAS', 'FUG', 'GAR', 'GDA', 'GMD', 'GNX', 'HAM', 'HOK', 'HOT', 'HRZ', 'HUD', 'IMX', 'IPM', 'ISE', 'JLC', 'KNK', 'KNM', 'KSK', 'KUR', 'MCN', 'MEW', 'MIC', 'MIL', 'NCS', 'NIC', 'NTB', 'ONS', 'PHA', 'PON', 'POP', 'QSR', 'RDM', 'RIV', 'ROM', 'SAC', 'SKP', 'SNT', 'SPS', 'SQE', 'STW', 'SYS', 'TAE', 'TEL', 'THR', 'TZG', 'WIN', 'XTA', 'ZOM'
```
The code can be optionally preceeded with P, seen in:
```
 'ANM', 'ART', 'COM', 'DBS', 'FAL', 'FUG', 'GAR', 'GNX', 'HAM', 'IRI', 'KSK', 'MCV', 'MIC', 'SAC', 'SYS', 'TAE', 'TEL', 'TEN', 'TIT'
```

"Y" is the initial number, dunno what it's used for. Seen values 1 to 5, inclusive.

"Z" is the game number, 0-999.

"a" is the package content type. Types are:
 'a' (application), 'm' (manual), 'd' (music)

The (P|)XXXYZZZ make up the game code and can be seen in the filename of the screenshots on the respectful games' page,
preceeded by "E". (this is identical to the executable name)

URLs start with http://www.amusement-center.com/productfiles/EGGFILES/ and have the filename directly after.

## Package Format
All strings are Shift-JIS encoded.
Using COM0004a.bin as an example:

```
0000h: 43 4E 50 46 56 52 42 35 B7 81 15 00 7E 00 00 00  CNPFVRB5·...~...
0010h: 04 00 00 00 43 4F 4D 30 30 30 34 5C 65 72 72 61  ....COM0004\erra 
0020h: 6E 64 2E 65 78 65 2C 64 61 74 61 2C 43 4F 4D 30  nd.exe,data,COM0 
0030h: 30 30 34 5C 45 47 47 8B 96 91 F8 2E 74 78 74 2C  004\EGG許諾.txt, 
0040h: 6B 79 6F 64 61 6B 75 2C 43 4F 4D 30 30 30 34 5C  kyodaku,COM0004\ 
0050h: 45 43 4F 4D 30 30 30 34 2E 54 58 54 2C 72 65 61  ECOM0004.TXT,rea 
0060h: 64 6D 65 2C 43 4F 4D 30 30 30 34 5C 45 43 4F 4D  dme,COM0004\ECOM 
0070h: 30 30 30 34 2E 45 58 45 2C 67 61 6D 65 2C        0004.EXE,game,
```

```c++
struct Header{
    char idstring[8]; //all have the same format afaik: CNPFVR06, CNPFVR02, CNPFVR07, CNPFVRB5; only seen B5 and 02
    int filesize;
    int dataoff;
    int files;
} header;

char filenamecsv[header.dataoff - 0x14]; #csv in the format [filename],[type],

struct Files {
    byte comp;
    byte type;
    int size;
    if( comp == 1 ){
        int decsize;
        uchar data[size - 4];}
    else{
        uchar data[size];
    }
} file[header.files];
```

A working QuickBMS script:
```
comtype saint_seya
idstring "CNPFVR"
get VER short
get FSIZE long
get DATAOFF long
get FILEZ long
for i = 0 < FILEZ
	getct NAME string 0x2C
	putarray 0 -1 NAME
	getct DESC string 0x2C
next i

goto DATAOFF
for i = 0 < FILEZ
	get COMP byte
	get TYPE byte
	get SIZE long
	savepos OFF
	getarray NAME 0 i
	if COMP == 1
		get DECS long
		savepos OFF
		math SIZE - 4
		clog NAME OFF SIZE DECS
	else
		log NAME OFF SIZE
	endif
	math OFF + SIZE
	goto OFF
next i
```

## Encryption
Use QuickBMS in conjunction with the "quickbms_project_egg_extract_bins_working.bms" script.

For EXEs that complain about missing files, find `Dr0Wy3K` in the EXE and replace the bytes afterward with `5D0A317081D120B0F100F4B5` or `1204E1F0E061` and null out the `20` before the `:`, 0x33 bytes after the bytes you replaced.
This may also be `CBy3fc3` in which case replace the bytes afterward with 
```
710171202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020203A20202020202020202020202020202020202020203AD80C54C574E541D55465416571803A7F1023D365D29120A1B051A1F11081E414E4
```