```
Author: Yixiong Ding, The University of Melborune
Date : 14.04.2018
```

import json
import mpi4py.MPI as MPI
import time
start = time.clock()

comm = MPI.COMM_WORLD
comm_rank = comm.Get_rank()
comm_size = comm.Get_size()

class melbGrid:
    id = ''
    xmin = 0
    xmax = 0
    ymin = 0
    ymax = 0
    postnum = 0
    def __init__(self, id, xmin, xmax, ymin, ymax, postnum):
        self.id = id
        self.xmin = xmin
        self.xmax = xmax
        self.ymin = ymin
        self.ymax = ymax
        self.postnum = postnum

class melbGrid_Row:
    row = ''
    postnum = 0
    def __init__(self, row, postnum):
        self.row = row
        self.postnum = postnum

class melbGrid_Col:
    col = ''
    postnum = 0
    def __init__(self, col, postnum):
        self.col = col
        self.postnum = postnum

## This function is used to read in the coordinates information, and retrun the list of grid_boxes objects and number of objects
def getGrid(grid_boxes, boxnum):
    with open("Melbourne-Area-Grid.json","r") as grid_file:
        for line in grid_file:
            if 'id' in line:

                ## Remove ',' and '\n' at the end of each line in the file
                data = json.loads(line.rstrip(',\r\n'))
                properties = data['properties']
                grid_boxes.append(melbGrid(properties['id'], properties['xmin'],properties['xmax'],properties['ymin'],properties['ymax'],0))
                boxnum += 1
    return grid_boxes, boxnum

## This function is used to check if a float number x in the range of y to z
def frange(x, y, z):
    if x >= y and x < z:
        return True
    else:
        return False

## This function is used to read in and process the Instagram file, and allocate the job for each process if using mpi
def getInsData(grid_boxes,boxnum):
    if comm_rank == 0:
        line_count = -1
        for line_count, line in enumerate(open("Sample-Instagram.json","rU")) :
            pass
        line_count += 1

    ## Each process will calculate its own job size and start line
    line_count = comm.bcast(line_count if comm_rank == 0 else None, root=0)
    job_size = line_count/comm_size
    start_line = comm_rank*job_size
    end_line = start_line+job_size

    cursor = 0
    finish = False

    with open("Sample-Instagram.json", 'rU') as ins:
     for line in ins:
            cursor += 1
            while cursor > start_line and finish == False:
                if 'id' in line:
                    coor_exist = json.JSONDecoder().raw_decode(line)[0]['doc'].get('coordinates',None)
                    if coor_exist:
                        coordinates = coor_exist['coordinates']
                        for i in range(boxnum):
                            if frange(coordinates[1],grid_boxes[i].xmin, grid_boxes[i].xmax) and frange(coordinates[0],grid_boxes[i].ymin, grid_boxes[i].ymax):
                                    grid_boxes[i].postnum += 1
                                    break
                if cursor == end_line:
                    finish = True
                break

## This function is used to collect and process the data from the list and print out the final results
def printResult(grid_boxes, boxnum):
    row_postnum = [melbGrid_Row('A-Row',0),melbGrid_Row('B-Row',0),melbGrid_Row('C-Row',0),melbGrid_Row('D-Row',0)]
    col_postnum = [melbGrid_Col('Column 1',0),melbGrid_Col('Column 2',0),melbGrid_Col('Column 3',0),melbGrid_Col('Column 4',0),melbGrid_Col('Column 5',0)]

    for i in range(boxnum):
        if grid_boxes[i].id[0] == 'A':
            row_postnum[0].postnum += grid_boxes[i].postnum
        if grid_boxes[i].id[0] == 'B':
            row_postnum[1].postnum += grid_boxes[i].postnum
        if grid_boxes[i].id[0] == 'C':
            row_postnum[2].postnum += grid_boxes[i].postnum
        if grid_boxes[i].id[0] == 'D':
            row_postnum[3].postnum += grid_boxes[i].postnum
    for i in range(boxnum):
        if grid_boxes[i].id[1] == '1':
            col_postnum[0].postnum += grid_boxes[i].postnum
        if grid_boxes[i].id[1] == '2':
            col_postnum[1].postnum += grid_boxes[i].postnum
        if grid_boxes[i].id[1] == '3':
            col_postnum[2].postnum += grid_boxes[i].postnum
        if grid_boxes[i].id[1] == '4':
            col_postnum[3].postnum += grid_boxes[i].postnum
        if grid_boxes[i].id[1] == '5':
            col_postnum[4].postnum += grid_boxes[i].postnum

    combine_grid = comm.gather(grid_boxes,root=0)
    combine_row = comm.gather(row_postnum,root=0)
    combine_col = comm.gather(col_postnum,root=0)

    if comm_rank == 0:
        for i in range(1,comm_size):
            for j in range(boxnum):
                if combine_grid[0][j].id == combine_grid[i][j].id:
                    combine_grid[0][j].postnum += combine_grid[i][j].postnum
            for k in range(4):
                if combine_row[0][k].row == combine_row[i][k].row:
                    combine_row[0][k].postnum += combine_row[i][k].postnum
            for l in range(5):
                if combine_col[0][l].col == combine_col[i][l].col:
                    combine_col[0][l].postnum += combine_col[i][l].postnum

        combine_grid[0].sort(key = lambda melbGrid: melbGrid.postnum, reverse = True)
        combine_row[0].sort(key = lambda melbGrid_Row: melbGrid_Row.postnum, reverse = True)
        combine_col[0].sort(key = lambda melbGrid_Col: melbGrid_Col.postnum, reverse = True)

        print '\nThe total number of Instagram posts made in each grid box'
        for i in range(boxnum):
            print combine_grid[0][i].id,':',combine_grid[0][i].postnum,'posts' 
        print '\nThe total number of Instagram posts made in each row'
        for i in range(4):
            print combine_row[0][i].row,':',combine_row[0][i].postnum,'posts'
        print '\nThe total number of Instagram posts made in each column'
        for i in range(5):
            print combine_col[0][i].col,':',combine_col[0][i].postnum,'posts'

## The main function of the program
def main():
    grid_boxes = []
    boxnum = 0
    grid_boxes,boxnum = getGrid(grid_boxes,boxnum)
    getInsData(grid_boxes,boxnum)
    printResult(grid_boxes,boxnum)
    if comm_rank == 0:
        elapsed = (time.clock() - start)
        print '\nTime used:',elapsed

if __name__ == "__main__":
    main()
