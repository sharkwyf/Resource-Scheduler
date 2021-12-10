#define _CRT_SECURE_NO_WARNINGS
#include "../hFiles/ResourceScheduler.h"
#define MAX_JOB_CORES 10


ResourceScheduler::ResourceScheduler(int tasktype,int caseID) {
	taskType = tasktype;
	string filePath = "C:/Users/12210/Desktop/Projects/5.Resource-Scheduler/Debug/input/task" + to_string(taskType) + "_case"+to_string(caseID)+".txt";
	//string filePath = "../input/task" + to_string(taskType) + "_case" + to_string(caseID) + ".txt";
	freopen(filePath.c_str(), "r", stdin);
	cin >> numJob >> numHost >> alpha;
	if (taskType == 2)
		cin >> St;
	hostCore.resize(numHost);
	for (int i = 0; i < numHost; i++)
		cin >> hostCore[i];

	jobBlock.resize(numJob);
	for (int i = 0; i < numJob; i++)
		cin >> jobBlock[i];

	Sc.resize(numJob);
	for (int i = 0; i < numJob; i++)
		cin >> Sc[i];

	dataSize.resize(numJob);
	for (int i = 0; i < numJob; i++) {
		dataSize[i].resize(jobBlock[i]);
		for (int j = 0; j < jobBlock[i]; j++)
			cin >> dataSize[i][j];
	}

	location.resize(numJob);
	for (int i = 0; i < numJob; i++) {
		location[i].resize(jobBlock[i]);
		for (int j = 0; j < jobBlock[i]; j++)
			cin >> location[i][j];
	}

	jobFinishTime.resize(numJob, 0);
	jobCore.resize(numJob);

	runLoc.resize(numJob);
	for (int i = 0; i < numJob; i++)
		runLoc[i].resize(jobBlock[i]);

	hostCoreTask.resize(numHost);
	for (int i = 0; i < numHost; i++)
		hostCoreTask[i].resize(hostCore[i]);

	hostCoreFinishTime.resize(numHost);
	for (int i = 0; i < numHost; i++)
		hostCoreFinishTime[i].resize(hostCore[i], 0);
}

vector<vector<double>>* pDataSize;
vector<double>* pDataSizeRow;
//vector<int>* pJobBlock;

bool compare2(const int& a, const int& b) {
	return (*pDataSizeRow)[a] > (*pDataSizeRow)[b];
}

bool compare3(const pair<int, vector<int>>& a, const pair<int, vector<int>>& b) {
	return (*pDataSize)[a.first][a.second[0]] > (*pDataSize)[b.first][b.second[0]];
	//return  accumulate((*pDataSize)[a.first].begin(), (*pDataSize)[a.first].end(), 0) > accumulate((*pDataSize)[b.first].begin(), (*pDataSize)[b.first].end(), 0);
	//return (*pJobBlock)[a.first] > (*pJobBlock)[b.first];
}

struct MyPair {
	int first;
	double second;
	vector<int> third;
};

class MyPairCompare {
public:
	bool operator()(const MyPair& left, const MyPair& right) const {
		if (left.second != right.second)
			return left.second < right.second;
		else
			return left.first < right.first;
	}
};

//O(numJob * MAX_JOB_CORES * m * jobBlock[i])
void ResourceScheduler::schedule() {

	vector<vector<int>> hostCoreBlock(numHost);
	for (int i = 0; i < numHost; i++)
		hostCoreBlock[i].resize(hostCore[i], 0);

	//The num of Cores in total. O(numHost)
	int m = 0;
	for (int num : hostCore) m += num;

	//Store the index of Jobs and Blocks. O(sum(jobBlock))
	vector<pair<int, vector<int>>> orderedJobs(numJob);
	for (int i = 0; i < numJob; i++) {
		orderedJobs[i].first = i;
		orderedJobs[i].second = vector<int>(jobBlock[i]);
		for (int j = 0; j < jobBlock[i]; j++) {
			orderedJobs[i].second[j] = j;
		}
	}

	//Sort the blocks of each job in order of BlockSize Desc. O(numJob * jobBlock[i] * log(jobBlock[i]))
	for (int i = 0; i < orderedJobs.size(); i++) {
		pDataSizeRow = &(dataSize[i]);
		sort(orderedJobs[i].second.begin(), orderedJobs[i].second.end(), compare2);
	}

	//Sort the jobs in order of Max BlockSize Desc. O(numJob * log(numJob))
	pDataSize = &(dataSize); 
	//pJobBlock = &(jobBlock);
	sort(orderedJobs.begin(), orderedJobs.end(), compare3);

	//Assign a 1-D index to each core of each host. O(m) // m is the total count of cores
	vector<pair<int, int>> coreLoc(m);
	int k = 0;
	for (int i = 0; i < numHost; i++) {
		for (int j = 0; j < hostCore[i]; j++)
			coreLoc[k++] = { i, j };
	}

	//Maintain a sequence of cores in order of FinishedTime Asc. O(m * log(m))
	set<MyPair, MyPairCompare> Set;
	for (int i = 0; i < m; i++) {
		Set.insert(MyPair{ i, 0.0 });
	}

	//Allocate the jobs in order. O(numJob * MAX_JOB_CORES * m * log(m) * jobBlock[i] * log(MAX_JOB_CORES))
	for (int i = 0; i < numJob; i++) {
		//set<pair<int, int>> allocatedJobCore;
		double minFinishTime = -1;
		double startTime = -1;
		int jobId = orderedJobs[i].first;

		//Consider to split the jobs into j parts
		int maxIt = min(min(m, jobBlock[orderedJobs[i].first]), MAX_JOB_CORES);
		vector<double> time;
		int j = 1;
		int prevj = 1;
		set<MyPair, MyPairCompare> assignedCores;
		set<int> assignedJobCores;
		set<MyPair, MyPairCompare> prevAssignedCores;

		//Allocate job blocks to j cores individually. O(MAX_JOB_CORES * m * log(m) * jobBlock[i] * log(MAX_JOB_CORES))
		for (; j < maxIt + 1; j++) {
			//Choose the core with the earliest FinishTime, then the block in use 
			//TODO: This could be optimized
			assignedCores.clear();
			assignedJobCores.clear();
			int transmitSpeed = St;
			int computingSpeed = (1 - alpha * (j - 1)) * Sc[jobId];

			//Allocate job blocks to j cores individually. O(m * log(m) * jobBlock[i] * log(MAX_JOB_CORES))
			for (int k = 0; k < jobBlock[jobId]; k++) {
				int blockId = orderedJobs[i].second[k];

				//Choose the core to assign
				//TODO: Optimize: May choose the cores with the most storage of blocks
				//If the assigned cores aren't full
				if (assignedCores.size() < j) {
					//Add new core. O(m)
					double minFinishTime1 = (Set.begin())->second;
					bool isStored = false;
					auto it = Set.begin();
					for (; it != Set.end(); it++) {
						if (assignedJobCores.find(it->first) != assignedJobCores.end()) {
							continue;
						}
						else if (it->second != minFinishTime1) {
							it = Set.begin();
							while (assignedJobCores.find(it->first) != assignedJobCores.end()) {
								it++;
							}
							break;
						}
						else {
							//If the host of core is the same as the job block is assigned
							if (coreLoc[it->first].first == location[jobId][blockId]) {
								isStored = true;
								break;
							}
						}
					}
					if (it == Set.end()) {
						it = Set.begin();
						while (assignedJobCores.find(it->first) != assignedJobCores.end()) {
							it++;
						}
					}
					//O(log(MAX_JOB_CORES))
					assignedJobCores.insert(it->first);
					assignedCores.insert(MyPair{ it->first, dataSize[jobId][blockId] / computingSpeed + (isStored ? 0 : dataSize[jobId][blockId] / transmitSpeed), vector<int> { blockId } });
				}
				else {
					//Choose the core that stores the block data among the cores with the same earliest FinishTime.
					bool isStored = false;
					auto it = assignedCores.begin();
					for (; it != assignedCores.end(); it++) {
						//If finishTime > minFinishTime1 + transmitTime, break the iteration
						if (it->second > assignedCores.begin()->second + dataSize[jobId][blockId] / transmitSpeed) {
							it = assignedCores.begin();
							break;
						}
						else {
							//If the host of core is the same as the job block is assigned
							if (coreLoc[it->first].first == location[jobId][blockId]) {
								isStored = true;
								break;
							}
						}
					}
					if (it == assignedCores.end())
						it = assignedCores.begin();
					//Update assignedCores
					MyPair p{ it->first, it->second + dataSize[jobId][blockId] / computingSpeed + (isStored ? 0 : dataSize[jobId][blockId] / transmitSpeed), it->third };
					p.third.push_back(blockId);
					assignedCores.erase(*it);
					assignedCores.insert(p);
				}
			}

			double maxStartTime1 = 0, maxFinishTime1 = 0;
			for (auto it = assignedCores.begin(); it != assignedCores.end(); it++) {
				int hostid = coreLoc[it->first].first;
				int coreid = coreLoc[it->first].second;
				maxStartTime1 = max(maxStartTime1, hostCoreFinishTime[hostid][coreid]);
				maxFinishTime1 = max(maxFinishTime1, it->second);
			}

			//Compare the MAKESPAN, if the new ieration has equal or lesser MAKESPAN, then update the assignment.
			if ((j == 1) || ((j > 1) && (maxStartTime1 + maxFinishTime1 < minFinishTime))) {
				prevj = j;
				startTime = maxStartTime1;
				minFinishTime = maxStartTime1 + maxFinishTime1;
				prevAssignedCores = assignedCores;
			}
		}

		//Record Data. O(jobBlock[i] + MAX_JOB_CORES * log(m))
		for (auto it = prevAssignedCores.begin(); it != prevAssignedCores.end(); it++) {
			int hostid = coreLoc[it->first].first;
			int coreid = coreLoc[it->first].second;

			//double stime = hostCoreFinishTime[hostid][coreid];
			double stime = startTime;
			int blockId = -1;
			// For job-block, Record the (host, core, rank): the order of execution of the block on the core. O(jobBlock[i])
			for (int k = 0; k < it->third.size(); k++) {

				blockId = it->third[k];
				runLoc[jobId][blockId] = make_tuple(hostid, coreid, hostCoreBlock[hostid][coreid]++);

				//Core perspective: host->core->task-> <job,block,startTime,endTime>
				double tptime = dataSize[jobId][blockId] / ((1 - alpha * (prevAssignedCores.size() - 1)) * Sc[jobId]) + (coreLoc[it->first].first == location[jobId][blockId] ? 0 : dataSize[jobId][blockId] / St);
				hostCoreTask[hostid][coreid].push_back( make_tuple(jobId, blockId, stime, stime + tptime));
				stime += tptime;
			}

			//Calculate Job FInish Time. O(1)
			jobFinishTime[jobId] = minFinishTime;

			//Record the number of cores allocated to the job. O(1)
			jobCore[jobId] = prevAssignedCores.size();

			//Record the finish time of host-core. O(MAX_JOB_CORES * log(m))
			hostCoreFinishTime[hostid][coreid] = minFinishTime;
			for (auto itS = Set.begin(); itS != Set.end(); itS++) {
				if (itS->first == it->first) {
					MyPair p{ it->first, minFinishTime };
					Set.erase(*itS);
					Set.insert(p);
					break;
				}
			}
		}
		cout << i << ". Job" << jobId << " : starts at |" << startTime << "|, ends at |" << minFinishTime << "|, uses " << prevAssignedCores.size() << " cores: {";
		for (auto it = prevAssignedCores.begin(); it != prevAssignedCores.end(); it++) {
			cout << it->first << ", ";
		}
		cout << "}" << endl;
	}

	//Examine Data

}



void ResourceScheduler::outputSolutionFromBlock() {
	cout << "\nTask2 Solution (Block Perspective) of Teaching Assistant:\n\n";
	for (int i = 0; i < numJob; i++) {
		double speed = g(jobCore[i]);
		cout << "Job" << i << " obtains " << jobCore[i] << " cores (speed=" << speed << ") and finishes at time " << jobFinishTime[i] << ": \n";
		for (int j = 0; j < jobBlock[i]; j++) {
			cout << "\tBlock" << j << ": H" << get<0>(runLoc[i][j]) << ", C" << get<1>(runLoc[i][j]) << ", R" << get<2>(runLoc[i][j]) << " (time=" << fixed << setprecision(2) << dataSize[i][j] / speed << ")" << " \n";
		}
		cout << "\n";
	}

	cout << "The maximum finish time: " << *max_element(jobFinishTime.begin(), jobFinishTime.end()) << "\n";
	cout << "The total response time: " << accumulate(jobFinishTime.begin(), jobFinishTime.end(), 0.0) << "\n\n";
}

void ResourceScheduler::outputSolutionFromCore() {
	cout << "\nTask2 Solution (Core Perspective) of Teaching Assistant:\n\n";
	double maxHostTime = 0, totalRunningTime = 0.0;
	for (int i = 0; i < numHost; i++) {
		double hostTime = *max_element(hostCoreFinishTime[i].begin(), hostCoreFinishTime[i].end());
		maxHostTime = max(hostTime, maxHostTime);
		totalRunningTime += accumulate(hostCoreFinishTime[i].begin(), hostCoreFinishTime[i].end(), 0.0);
		cout << "Host" << i << " finishes at time " << hostTime << ":\n\n";
		for (int j = 0; j < hostCore[i]; j++) {
			cout << "\tCore" << j << " has " << hostCoreTask[i][j].size() << " tasks and finishes at time " << hostCoreFinishTime[i][j] << ":\n";
			for (int k = 0; k < hostCoreTask[i][j].size(); k++) {
				cout << "\t\tJ" << setw(2) << setfill('0') << get<0>(hostCoreTask[i][j][k]) << ", B" << setw(2) << setfill('0') << get<1>(hostCoreTask[i][j][k]) << ", runTime " << fixed << setprecision(1) << setw(5) << setfill('0') << get<2>(hostCoreTask[i][j][k]) << " to " << fixed << setprecision(1) << setw(5) << setfill('0') << get<3>(hostCoreTask[i][j][k]) << "\n";
			}
			cout << "\n";
		}
		cout << "\n\n";
	}
	cout << "The maximum finish time of hosts: " << maxHostTime << "\n";
	cout << "The total efficacious running time: " << totalRunningTime << "\n";
	cout << "Utilization rate: " << totalRunningTime / accumulate(hostCore.begin(), hostCore.end(), 0.0) / maxHostTime << "\n\n";
}

void ResourceScheduler::visualization() {

}

double ResourceScheduler::g(int e) {
	return 1 - alpha * (e - 1);
}
