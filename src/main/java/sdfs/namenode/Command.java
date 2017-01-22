package sdfs.namenode;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * Created by lenovo on 2016/11/20.
 */
class Command implements Serializable{
    //command type
    private CommandType type;
    //command uuid
    private UUID uuid;
    //command path
    private String path;
    //command phase
    private Phase phase;
    Command(CommandType type, UUID uuid, Phase phase, String path) {
        this.type = type;
        this.uuid = uuid;
        this.phase = phase;
        this.path = path;
    }


    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public CommandType getType() {
        return type;
    }

    public void setType(CommandType type) {
        this.type = type;
    }

    public Phase getPhase() {
        return phase;
    }

    public void setPhase(Phase phase) {
        this.phase = phase;
    }
    //commit validation judging whether there are two transaction conflicting with each other
    static boolean isMKDIRValid(Queue<Command> commands, Command command) {
        List<Command> com = new ArrayList<>();
        Iterator<Command> iterator = commands.iterator();
        iterator.forEachRemaining(com::add);
        Map<UUID, Boolean> results = new HashMap<>();
        for (Command aCom : com) {
            if (aCom.path.equals(command.path) && aCom.uuid != command.uuid && aCom.phase != Phase.END) {
                if (aCom.type == CommandType.CREATE || aCom.type == CommandType.MKDIR || aCom.type == CommandType.RM || aCom.type == CommandType.RENAME)
                    results.put(aCom.uuid, false);
            }
            if (aCom.path.equals(command.path) && aCom.uuid != command.uuid && aCom.phase == Phase.END) {
                results.put(aCom.uuid, true);
                garbageCollecting(commands, aCom);
            }
            if (aCom == command)
                break;
        }
        return !results.containsValue(false);
    }
    //delete validating rules
    static boolean isDELETEValid(Queue<Command> commands, Command command) {
        List<Command> com = new ArrayList<>();
        Iterator<Command> iterator = commands.iterator();
        iterator.forEachRemaining(com::add);
        Map<UUID, Boolean> results = new HashMap<>();
        for (Command aCom : com) {
            if (aCom.path.equals(command.path) && aCom.uuid != command.uuid && aCom.phase != Phase.END) {
                if (aCom.type == CommandType.CREATE || aCom.type == CommandType.OPENREADWRITE || aCom.type == CommandType.OPENONLYREAD || aCom.type == CommandType.RENAME)
                    results.put(aCom.uuid, false);
            }
            if (aCom.path.equals(command.path) && aCom.uuid != command.uuid && aCom.phase == Phase.END) {
                results.put(aCom.uuid, true);
                garbageCollecting(commands, aCom);
            }
            if (aCom == command)
                break;
        }
        return !results.containsValue(false);
    }
    //rename validating rules
    static boolean isRENAMEValid(ConcurrentLinkedQueue<Command> commands, Command command) {
        List<Command> com = new ArrayList<>();
        Iterator<Command> iterator = commands.iterator();
        iterator.forEachRemaining(com::add);
        Map<UUID, Boolean> results = new HashMap<>();
        for (Command aCom : com) {
            if (aCom.path.equals(command.path) && aCom.uuid != command.uuid && aCom.phase != Phase.END) {
                if (aCom.type == CommandType.CREATE || aCom.type == CommandType.OPENREADWRITE || aCom.type == CommandType.OPENONLYREAD ||
                        aCom.type == CommandType.RM || aCom.type == CommandType.RENAME)
                    results.put(aCom.uuid, false);
            }
            if (aCom.path.equals(command.path) && aCom.uuid != command.uuid && aCom.phase == Phase.END) {
                results.put(aCom.uuid, true);
                garbageCollecting(commands, aCom);
            }
            if (aCom == command)
                break;
        }
        return !results.containsValue(false);
    }
    private static void garbageCollecting(Queue<Command> commands, Command command) {
        commands.removeAll(commands.stream().filter(command1 -> command1.uuid == command.uuid).collect(Collectors.toList()));
    }

    enum CommandType {
        MKDIR, OPENONLYREAD, OPENREADWRITE, CREATE, RM, RENAME
    }
    enum Phase {
        BEGIN, MODIFY, COMMIT, END, ABORT
    }
}
