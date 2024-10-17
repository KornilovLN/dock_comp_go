package main

import (
    "context"
    "fmt"
    "net/http"
    "os"
    "strconv"
    "time"

    "github.com/gin-gonic/gin"
    "github.com/go-redis/redis/v8"
    "github.com/google/uuid"
)

type Task struct {
    Id          string `json:"id"`
    Name        string `json:"name"`
    Description string `json:"description"`
    Timestamp   int64  `json:"timestamp"`
    TaskerId    string `json:"tasker_id"`
    WorkerId    string `json:"worker_id"`
}

var (
    client = redis.NewClient(&redis.Options{
        Addr:     getStrEnv("REDIS_HOST", "localhost:6379"),
        Password: getStrEnv("REDIS_PASSWORD", ""),
        DB:       getIntEnv("REDIS_DB", 0),
    })
)

func setupRouter() *gin.Engine {
    r := gin.Default()

    r.GET("/ping", func(c *gin.Context) {
        c.String(http.StatusOK, "pong")
    })

    r.GET("/task", func(c *gin.Context) {
        if tasks, err := fetchTasks(c); err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
        } else {
            c.JSON(http.StatusOK, gin.H{"tasks": tasks})
        }
    })

    r.GET("/task/:id", func(c *gin.Context) {
        id := c.Params.ByName("id")
        if task, err := fetchTask(c.Request.Context(), id); err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"id": id, "message": err.Error()})
        } else if task == nil {
            c.JSON(http.StatusNotFound, gin.H{"id": id, "message": "not found"})
        } else {
            c.JSON(http.StatusOK, gin.H{"task": task})
        }
    })

    r.POST("/task", func(c *gin.Context) {
        var task Task
        if err := c.BindJSON(&task); err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"task": task, "created": false, "message": err.Error()})
            return
        }
        task.Id = uuid.New().String()
        task.Timestamp = time.Now().Unix()
        if err := persistTask(c, task); err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"task": task, "created": false, "message": err.Error()})
            return
        }
        c.JSON(http.StatusCreated, gin.H{"task": task, "created": true, "message": "Task Created Successfully"})
    })

    r.DELETE("/task/:id", func(c *gin.Context) {
        id := c.Params.ByName("id")
        if err := deleteTask(c.Request.Context(), id); err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"id": id, "message": err.Error()})
        } else {
            c.JSON(http.StatusOK, gin.H{"id": id, "message": "Task deleted"})
        }
    })

    return r
}

func persistTask(c context.Context, task Task) error {
    hmset := client.HSet(c, fmt.Sprintf("task:%s", task.Id), 
        "Id", task.Id, 
        "Name", task.Name, 
        "Description", task.Description, 
        "Timestamp", task.Timestamp,
        "TaskerId", task.TaskerId,
        "WorkerId", task.WorkerId,
    )

    if hmset.Err() != nil {
        return hmset.Err()
    }

    z := redis.Z{Score: float64(task.Timestamp), Member: task.Id}
    zadd := client.ZAdd(c, "tasks", &z)

    if zadd.Err() != nil {
        return zadd.Err()
    }

    return nil
}

func fetchTask(c context.Context, id string) (*Task, error) {
    hgetAll := client.HGetAll(c, fmt.Sprintf("task:%s", id))

    if err := hgetAll.Err(); err != nil {
        return nil, err
    }

    ires, err := hgetAll.Result()

    if err != nil {
        return nil, err
    }

    if l := len(ires); l == 0 {
        return nil, nil
    }

    timestamp, _ := strconv.ParseInt(ires["Timestamp"], 10, 64)
    task := Task{
        Id:          ires["Id"],
        Name:        ires["Name"],
        Description: ires["Description"],
        Timestamp:   timestamp,
        TaskerId:    ires["TaskerId"],
        WorkerId:    ires["WorkerId"],
    }
    return &task, nil
}

func deleteTask(c context.Context, id string) error {
    if err := client.Unlink(c, fmt.Sprintf("task:%s", id)).Err(); err != nil {
        return err
    }

    if err := client.ZRem(c, "tasks", id).Err(); err != nil {
        return err
    }

    return nil
}

func fetchTasks(c context.Context) ([]*Task, error) {
    var tasks []*Task = make([]*Task, 0)

    zRange := client.ZRange(c, "tasks", 0, -1)

    if err := zRange.Err(); err != nil {
        return nil, err
    }

    ids, err := zRange.Result()

    if err != nil {
        return nil, err
    }

    for _, id := range ids {
        if task, err := fetchTask(c, id); err != nil {
            return nil, err
        } else {
            tasks = append(tasks, task)
        }
    }

    return tasks, nil
}

func getIntEnv(key string, defaultvaule int) int {
    if value := os.Getenv(key); len(value) == 0 {
        return defaultvaule
    } else {
        if i, err := strconv.Atoi(value); err == nil {
            return i
        } else {
            return defaultvaule
        }
    }
}

func getStrEnv(key string, defaultValue string) string {
    if value := os.Getenv(key); len(value) == 0 {
        return defaultValue
    } else {
        return value
    }
}

//-------------------------------------------------------------------------------------

func createTask(ctx context.Context, taskerId, workerId, name, description string) {
    task := Task{
        Id:          uuid.New().String(),
        Name:        name,
        Description: description,
        Timestamp:   time.Now().Unix(),
        TaskerId:    taskerId,
        WorkerId:    workerId,
    }
    if err := persistTask(ctx, task); err != nil {
        fmt.Printf("Error adding task for %s: %v\n", workerId, err)
    } else {
        fmt.Printf("Added task for %s from %s\n", workerId, taskerId)
    }
}

func blockTasks() {
    ctx := context.Background()

    // Tasker1 tasks
    createTask(ctx, "tsk-1", "worker1", "Task for Worker1", "Description for Worker1")
    createTask(ctx, "tsk-1", "worker3", "Task for Worker3", "Description for Worker3")
    createTask(ctx, "tsk-1", "worker4", "Task for Worker4 from Tasker1", "Description for Worker4 from Tasker1")

    // Tasker2 tasks
    createTask(ctx, "tsk-2", "worker5", "Task for Worker5", "Description for Worker5")

    // Tasker3 tasks
    createTask(ctx, "tsk-3", "worker2", "Task for Worker2", "Description for Worker2")
    createTask(ctx, "tsk-3", "worker4", "Task for Worker4 from Tasker3", "Description for Worker4 from Tasker3")
}

func main() {
    r := setupRouter()

    blockTasks()

    r.Run(getStrEnv("TASK_MANAGER_HOST", ":8080"))
}


