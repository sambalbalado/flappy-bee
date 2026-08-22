/**
 * Reactive game logic and SVG rendering for Flappy Bee.
 * @author Esaam Bin Haji Matudin
 */

import "./style.css";

import {
    Observable,
    catchError,
    filter,
    fromEvent,
    interval,
    map,
    scan,
    switchMap,
    take,
    merge,
    ReplaySubject,
    pipe,
    tap,
    startWith,
    withLatestFrom,
} from "rxjs";
import { fromFetch } from "rxjs/fetch";

/** ---------------- CONSTANTS --------------- */

const Viewport = {
    CANVAS_WIDTH: 600,
    CANVAS_HEIGHT: 400,
} as const;

const Bee = {
    WIDTH: 42,
    HEIGHT: 20,
} as const;

const Heart = {
    SIZE: 18,
    SPEED: 5.5,
    FIRST_SPAWN_TIME: 13,
    MIN_SPAWN_DELAY: 14,
    SPAWN_DELAY_VARIATION: 7,
    BOB_DISTANCE: 38,
    BOB_SPEED: 5.5,
} as const;

const Constants = {
    PIPE_WIDTH: 50,
    TICK_RATE_MS: 20, // Might need to change this!
    GRAVITY: 0.5, 
    GROUND: Viewport.CANVAS_HEIGHT - Bee.HEIGHT,
    CEILING: 0, 
    HIT_COOLDOWN: 1, // seconds of invulnerability after a hit
    SEED: 1234,
    CLOUD_SPEED: 14,
    GROUND_SPEED: 80,
} as const;


/** ---------------- USER INPUT --------------- */

type Key = "Space" | "KeyR" | "Escape";


/** ---------------- PIPE TYPES --------------- */

type Pipe = Readonly<{
    gap_y: number; // Y position of the center of the gap
    gap_height: number; // Height of the gap
    time: number;
}>;

/** ---------------- STATE TYPES --------------- */

/* State of a single pipe */
type PipeState = Readonly<{
    x: number; 
    gap_y: number; 
    gap_height: number; 
}>;

/* Bee state */
type BeeState = Readonly<{
    y: number; 
    x: number; 
    velocity: number; 
}>;

type HeartState = Readonly<{
    x: number;
    baseY: number;
    phase: number;
}>;

// State processing
type State = Readonly<{
    gameEnd: boolean;
    bee: BeeState;
    pipes: PipeState[]
    time: number;
    lives: number;
    cooldown: number;
    score: number;
    currentPath : readonly number[]
    previousPaths: readonly number[][]
    paused: boolean
    hearts: readonly HeartState[];
    nextHeartSpawn: number;
    heartsSpawned: number;
}>;

/** ---------------- INITIAL STATE --------------- */

const initialState: State = {
    gameEnd: false,
    bee: {
      y: Viewport.CANVAS_HEIGHT / 2 - Bee.HEIGHT / 2,
      x: Viewport.CANVAS_WIDTH * 0.3 - Bee.WIDTH / 2,
      velocity: 0,
    },
    pipes: [],
    time: 0,
    lives: 3,
    cooldown: -Constants.HIT_COOLDOWN,
    score: 0,
    currentPath: [],
    previousPaths: [],
    paused: false,
    hearts: [],
    nextHeartSpawn: Heart.FIRST_SPAWN_TIME,
    heartsSpawned: 0,
  };


/** ---------------- UTILITIES --------------- */

/**
 * A random number generator which provides two pure functions
 * `hash` and `scale`. Call `hash` repeatedly to generate the
 * sequence of hashes.
 */
abstract class RNG {
    private static m = 0x80000000; // 2^31
    private static a = 1103515245;
    private static c = 12345;

    // Store the seed inside the class
    private static seed: number = Constants.SEED;

    public static hash = (seed: number): number =>
        (RNG.a * seed + RNG.c) % RNG.m;

    public static scale = (hash: number): number =>
        (2 * hash) / (RNG.m - 1) - 1; // in [-1, 1]

    // Generates the next random number in [-1, 1]
    public static nextRand(): number {
        RNG.seed = RNG.hash(RNG.seed);
        return RNG.scale(RNG.seed);
    }
}


/**
 * Parses the CSV file and returns an array of Pipe objects.
 * @param csv Csv file contents 
 * 
 * @returns Pipe objects 
 */
const parseCSV = (csv: string): readonly Pipe[] => {
const [_, ...lines] = csv.trim().split("\n");
return lines.map(line => {
    const [gap_y, gap_height, time] = line.split(",").map(Number);
    return { gap_y, gap_height, time };
});
};

/** 
 * Determines the bounce velocity based on the source of the collision.
 * @param source Source of the collision ('ground', 'ceiling', 'topPipe', 'bottomPipe')
 * @returns Bounce velocity
 */

const bounce: (source: 'ground' | 'ceiling' | 'topPipe' | 'bottomPipe') => number = 
(source) => {
    const rand = RNG.nextRand() * 2 + 8; // Change to RNG function in the future
    switch (source) {
      case 'ground':
      case 'bottomPipe':
        return -rand; // Bounce up
      case 'ceiling':
      case 'topPipe':
        return rand;  // Bounce down
    }
  }

/**
 * Conditionally updates a value based on a condition.
 * @param condition Condition to check
 * @param Fn Function to update the value if condition true
 * @returns Function that updates old value
 */
const conditionalUpdate = <T>(condition: boolean, Fn: (old: T) => T) => (old: T) =>
    condition ? Fn(old) : old;


/** ---------------- BEE FUNCTIONS --------------- */

/**
 * Checks if bee hits the ground.
 * @param y Bee y-coordinate
 * @returns True if bee hits the ground, else false
 */
const hitGround = (y: number): boolean => y === Constants.GROUND;


/**
 * Checks if bee hits the ceiling.
 * @param y Bee y-coordinate
 * @returns True if bee hits ceiling, else false
 */
const hitCeiling = (y: number): boolean => y === Constants.CEILING;


/**
 * Checks if bee hits canvas (ground or ceiling).
 * @param y Bee y-coordinate
 * @returns True if bee hits canvas, else false
 */
const hitCanvas = (y: number): boolean => hitGround(y) || hitCeiling(y);

/**
 * Applies gravity to velocity.
 * @param velocity Current velocity
 * @returns Velocity after gravity
 */
const applyGravity = (velocity: number): number => velocity + Constants.GRAVITY;

/**
 * Updates bee's position based on velocity.
 * @param y Current y-coordinate
 * @param velocity Bee velocity
 * @returns Updated y-coordinate
 */
const updatePosition = (y: number, velocity: number): number =>
    Math.min(Constants.GROUND, Math.max(Constants.CEILING, y + velocity));

/**
 * Calculates bee velocity after collision.
 * @param beeY Bee y-coordinate
 * @param hitPipe Pipe collided with, if any
 * @param pureVelocity Bee's velocity without collision
 * @returns New bee velocity
 */
const bounceVelocity = (beeY: number, 
    hitPipe: PipeState | undefined, 
    pureVelocity: number): number =>
    hitCanvas(beeY)
      ? bounce(hitGround(beeY) ? 'ground' : 'ceiling')
      : hitPipe
      ? bounce(beeY < hitPipe.gap_y ? 'topPipe' : 'bottomPipe')
      : pureVelocity;


/** ---------------- PIPE FUNCTIONS --------------- */

/**
 * Spawns new pipes based on CSV timing.
 * @param pipesFromCSV Array of CSV pipes
 * @param time Current time
 * @param newTime Time after tick
 * @returns Array of new PipeState objects
 */
const spawnNewPipes = (pipesFromCSV: readonly Pipe[], 
    time: number, 
    newTime: number): PipeState[] =>
    pipesFromCSV
      .filter(pipe => pipe.time >= time && pipe.time < newTime)
      .map(pipe => ({
        x: Viewport.CANVAS_WIDTH,
        gap_y: pipe.gap_y * Viewport.CANVAS_HEIGHT,
        gap_height: pipe.gap_height * Viewport.CANVAS_HEIGHT,
      }));

/**
 * Filters pipes that are offscreen.
 * @param pipes Array of PipeState objects
 * @returns Array of pipes that are still on screen
 */
const filterOffscreen = (pipes: readonly PipeState[]): PipeState[] => 
    pipes.filter(p => p.x + Constants.PIPE_WIDTH > 0);

/**
 * Moves a pipe left by 4 pixels.
 * @returns Function to update pipe position
 */
const movePipe = () => (pipe: PipeState): PipeState => ({ ...pipe, x: pipe.x - 4 });


/**
 * Updates all pipes for a tick.
 * @param pipes Current pipes
 * @param pipesFromCSV CSV pipe data
 * @param time Current time
 * @param newTime Time after tick
 * @returns Updated array of PipeState objects
 */
const updatePipes = (
    pipes: PipeState[], 
    pipesFromCSV: readonly Pipe[], 
    time: number, 
    newTime: number): PipeState[] =>
    filterOffscreen([
        ...pipes.map(movePipe()), 
        ...spawnNewPipes(pipesFromCSV, time, newTime)]);


/**
 * Checks if the bee collides with a specific pipe.
 * @param bee Current bee state
 * @returns Function that checks collision with given pipe
 */
const collidedWithPipe = (bee: BeeState) => (pipe: PipeState): boolean => {
    const withinX = 
        bee.x + Bee.WIDTH > pipe.x && 
        bee.x < pipe.x + Constants.PIPE_WIDTH;
    const outsideGap = 
        bee.y < pipe.gap_y - pipe.gap_height / 2 || 
        bee.y + Bee.HEIGHT > pipe.gap_y + pipe.gap_height / 2;
    return withinX && outsideGap;
    };

/**
 * Finds the pipe that the bee collides with.
 * @param bee Current bee state
 * @returns Function that takes array of pipes and returns colliding pipe
 */
const findCollidingPipe = (bee: BeeState) => (pipes: PipeState[])
: PipeState | undefined =>
    pipes.find(collidedWithPipe(bee));

/**
 * Checks if the bee collides with any pipe.
 * @param bee Current bee state
 * @returns Function that takes array of pipes and returns true if collision occurs
 */ 
const hasCollidedPipe = (bee: BeeState) => (pipes: PipeState[]): boolean =>
    pipes.some(collidedWithPipe(bee));

/**
 * Returns the list of pipes that have just been passed by the bee.
 * 
 * A pipe is considered passed if its right edge (x + width)
 * has just crossed the bee's x position in this frame.
 * 
 * @param beeX X position of the bee
 * @param pipes Current pipes on screen
 * @returns Array of passed pipes
 */

const getPassedPipes = (
    beeX: number, 
    pipes: readonly PipeState[])
    : PipeState[] => {
    return pipes.filter(
      p => (p.x + Constants.PIPE_WIDTH >= beeX) &&
           (p.x + Constants.PIPE_WIDTH <= beeX + 4) // bee moves 4px per tick
    );
  };


/** 
 * Gives boolean value whether or not 
 * the bee has crossed the last pipe from the CSV.
 * 
 * @param beeX X position of the bee
 * @param pipes Current pipes on screen
 * @param pipesFromCSV All pipes from the CSV
 * @returns True if the bee has crossed the last pipe, false otherwise
 */ 
const hasCrossedLastPipe = (
    beeX: number, 
    pipes: readonly PipeState[], 
    pipesFromCSV: readonly Pipe[])
    : boolean => {
        if (pipesFromCSV.length === 0) return false;
        // Get last line of CSV
        const lastCSV = pipesFromCSV[pipesFromCSV.length - 1];
    
        const lastPipe = pipes.find(
        p =>
            p.gap_y === lastCSV.gap_y * Viewport.CANVAS_HEIGHT &&
            p.gap_height === lastCSV.gap_height * Viewport.CANVAS_HEIGHT
        );
        // Check if the last pipe has been passed by bee
        return lastPipe ? getPassedPipes(beeX - 15, [lastPipe]).length > 0 : false;
  }


/** ---------------- COOLDOWN FUNCTIONS --------------- */

/**
 * Returns a function that sets cooldown to new time.
 * @param newTime Current time
 * @returns Function that sets cooldown
 */
const setCooldown = (newTime: number) => (_: number): number => newTime;

/**
 * Updates cooldown if bee hit a pipe and can take hit.
 * @param hitPipe True if hit pipe
 * @param canHit True if bee can take hit
 * @param newTime Current time
 * @returns Function to update cooldown
 */
const updateCooldown = (hitPipe: boolean, canHit: boolean, newTime: number) =>
    conditionalUpdate(hitPipe && canHit, setCooldown(newTime));

/** Returns boolean of whether if the bee can take a hit based on cooldown.
 * @param state Current state
 * @param newTime New time after tick
 * @returns True if the bee can take a hit, false otherwise
 */
const canTakeHit = (state: State, newTime: number): boolean =>
    (newTime - state.cooldown) > Constants.HIT_COOLDOWN;


/** ---------------- LIVES & SCORE FUNCTIONS --------------- */

/**
 * Decrements lives but ensures non-negative.
 * @param lives Current lives
 * @returns Updated lives
 */
const decrementLives = (lives: number): number => Math.max(0, lives - 1);

/**
 * Updates lives based on obstacles hit and cooldown.
 * @param obstaclesHit True if hit occurred
 * @param canHit True if can take hit
 * @returns Function to update lives
 */
const updateLives = (obstaclesHit: boolean, canHit: boolean) =>
    conditionalUpdate(obstaclesHit && canHit, decrementLives);

/**
 * Updates the score based on pipes passed.
 * @param beeX Bee x-coordinate
 * @param pipes Array of PipeState objects
 * @param currentScore Current score
 * @returns Updated score
 */
const updateScore = (
  
    beeX: number, 
    pipes: PipeState[], 
    currentScore: number,
    obstaclesHit: boolean,
    canHit: boolean,): number =>
        // Dont add score if bee cant get hit 
        // (Means has hit pipe previous second)
        (!canHit)
            ? currentScore
            : currentScore + getPassedPipes(beeX, pipes).length;

/** ---------------- HEART FUNCTIONS --------------- */

const heartY = (heart: HeartState, time: number): number =>
    heart.baseY + Math.sin(time * Heart.BOB_SPEED + heart.phase) * Heart.BOB_DISTANCE;

const beeCollectedHeart = (
    bee: BeeState,
    heart: HeartState,
    time: number,
): boolean => {
    const y = heartY(heart, time);
    return bee.x < heart.x + Heart.SIZE &&
        bee.x + Bee.WIDTH > heart.x &&
        bee.y < y + Heart.SIZE &&
        bee.y + Bee.HEIGHT > y;
};

const nextHeartDelay = (spawnNumber: number): number =>
    Heart.MIN_SPAWN_DELAY +
    ((RNG.hash(Constants.SEED + spawnNumber * 7919) >>> 0) %
        Heart.SPAWN_DELAY_VARIATION);

const createHeart = (spawnNumber: number): HeartState => {
    const hash = RNG.hash(Constants.SEED + spawnNumber * 3571) >>> 0;
    return {
        x: Viewport.CANVAS_WIDTH,
        // Keep the full bobbing path on-screen while varying each spawn.
        baseY: 70 + (hash % (Viewport.CANVAS_HEIGHT - 160)),
        phase: (hash % 628) / 100,
    };
};


/** ---------------- GAME FUNCTIONS --------------- */

/**
 * Determines if the game should end.
 * @param lives Remaining lives
 * @param beeX Bee x-coordinate
 * @param pipes Array of PipeState objects
 * @param pipesFromCSV Array of pipes from CSV
 * @param currentGameEnd Current game end status
 * @returns True if game ends, else false
 */
const checkGameEnd = (
    lives: number, 
    beeX: number, 
    pipes: PipeState[], 
    pipesFromCSV: readonly Pipe[], 
    currentGameEnd: boolean)
    : boolean =>
currentGameEnd || lives === 0 || hasCrossedLastPipe(beeX, pipes, pipesFromCSV);



/** ---------------- TICK  --------------- */
 
/**
 * 
 * Advances the game state by one tick.
 *
 * This function updates all dynamic aspects of the game:
 * - Time
 * - Pipe positions
 * - Bee position and velocity (gravity + collisions)
 * - Collision detection with canvas edges and pipes
 * - Lives and cooldown handling
 * - Score
 * - Game end status
 * - Bee path history for visualization
 *
 * @param pipesFromCSV Array of pipe definitions loaded from CSV
 * @returns the updated state
 */
const tick = (pipesFromCSV: readonly Pipe[]) => (s: State): State => {
    const newTime = s.time + Constants.TICK_RATE_MS / 1000;

    
    // Compute bees velocity after gravity
    const pureVelocity = applyGravity(s.bee.velocity);

    // Update bee y position
    const beeY = updatePosition(s.bee.y, pureVelocity);
  
    // Get the state of the pipe of the bee collided with
    const hitPipe = findCollidingPipe(s.bee)(s.pipes);

    // Check if bee hits with obstacles (canvas/pipes)
    const obstaclesHit = hitCanvas(beeY) || hasCollidedPipe(s.bee)(s.pipes);

    // Check if bee is not on cooldown
    const canHit = canTakeHit(s, newTime);
  
    // Decrement lives if bee collides
    const newLives = updateLives(obstaclesHit, canHit)(s.lives);

    // Update cooldown timer after hit
    const newCooldown = updateCooldown(!!hitPipe, canHit, newTime)(s.cooldown);
  
    // Final velocity of bee
    const beeV = bounceVelocity(beeY, hitPipe, pureVelocity);

    // Update score when passing pipes
    const newScore = updateScore(s.bee.x, s.pipes, s.score, obstaclesHit, canHit)

    // Hearts are intentionally small, faster than pipes, and bob sharply up/down.
    const movedHearts = s.hearts
        .map(heart => ({ ...heart, x: heart.x - Heart.SPEED }))
        .filter(heart => heart.x + Heart.SIZE > 0);
    const collectedHearts = movedHearts.filter(heart =>
        beeCollectedHeart({ ...s.bee, y: beeY }, heart, newTime));
    const remainingHearts = movedHearts.filter(heart =>
        !beeCollectedHeart({ ...s.bee, y: beeY }, heart, newTime));
    const shouldSpawnHeart = newTime >= s.nextHeartSpawn;
    const heartsSpawned = s.heartsSpawned + (shouldSpawnHeart ? 1 : 0);
    const hearts = shouldSpawnHeart
        ? [...remainingHearts, createHeart(heartsSpawned)]
        : remainingHearts;
    const livesAfterHearts = newLives + collectedHearts.length;
    const nextHeartSpawn = shouldSpawnHeart
        ? newTime + nextHeartDelay(heartsSpawned)
        : s.nextHeartSpawn;

    // Check if game end requirement is met
    const gameEnd = checkGameEnd(
        livesAfterHearts, s.bee.x, s.pipes, pipesFromCSV, s.gameEnd);
  
    return { 
        ...s,
        time: newTime, 
        pipes: s.pipes, 
        bee: { ...s.bee, velocity: beeV, y: beeY }, 
        lives: livesAfterHearts,
        cooldown: newCooldown, 
        score: newScore, 
        gameEnd,
        currentPath: [...s.currentPath, beeY],
        hearts,
        nextHeartSpawn,
        heartsSpawned};
  };


/** ---------------- Rendering (side effects) --------------- */

/**
 * Brings an SVG element to the foreground.
 * @param elem SVG element to bring to the foreground
 */
const bringToForeground = (elem: SVGElement): void => {
    elem.parentNode?.appendChild(elem);
};

/**
 * Displays a SVG element on the canvas. Brings to foreground.
 * @param elem SVG element to display
 */
const show = (elem: SVGElement): void => {
    elem.setAttribute("visibility", "visible");
    bringToForeground(elem);
};

/**
 * Hides a SVG element on the canvas.
 * @param elem SVG element to hide
 */
const hide = (elem: SVGElement): void => {
    elem.setAttribute("visibility", "hidden");
};

/** ---------------- SVG elements --------------- */

/**
 * Creates an SVG element with the given properties.
 *
 * See https://developer.mozilla.org/en-US/docs/Web/SVG/Element for valid
 * element names and properties.
 *
 * @param namespace Namespace of the SVG element
 * @param name SVGElement name
 * @param props Properties to set on the SVG element
 * @returns SVG element
 */
const createSvgElement = (
    namespace: string | null,
    name: string,
    props: Record<string, string> = {},
): SVGElement => {
    const elem = document.createElementNS(namespace, name) as SVGElement;
    Object.entries(props).forEach(([k, v]) => elem.setAttribute(k, v));
    return elem;
};

/** ---------------- RENDER --------------- */

/**
 * Creates a renderer function that updates the game visuals on the SVG canvas.
 * @returns A function that takes the current State and updates the SVG canvas accordingly.
 * 
 */
const render = (): ((s: State) => void) => {

    // Canvas elements
    const gameOver = document.querySelector("#gameOver") as SVGElement;
    const container = document.querySelector("#main") as HTMLElement;

    // Pause element and hide at the start
    const pause = document.querySelector("#pause") as SVGElement;
    hide(pause)

    // Text fields
    const livesText = document.querySelector("#livesText") as HTMLElement;
    const scoreText = document.querySelector("#scoreText") as HTMLElement;

    const svg = document.querySelector("#svgCanvas") as SVGSVGElement;
    const cloudScroller = document.querySelector("#cloudScroller") as SVGElement;
    const groundScroller = document.querySelector("#groundScroller") as SVGElement;
    svg.setAttribute(
        "viewBox",
        `0 0 ${Viewport.CANVAS_WIDTH} ${Viewport.CANVAS_HEIGHT}`,
    );

    // Add bee to the main grid canvas
    const beeImg = createSvgElement(svg.namespaceURI, "image", {
        href: "assets/bee.png",
        x: `${Viewport.CANVAS_WIDTH * 0.3 - Bee.WIDTH / 2}`,
        y: `${initialState.bee.y}`,//`${Viewport.CANVAS_HEIGHT / 2 - Bee.HEIGHT / 2}`,
        width: `${Bee.WIDTH}`,
        height: `${Bee.HEIGHT}`,
    });
    svg.appendChild(beeImg);

    /**
     * Renders the current state to the canvas.
     * In MVC terms, this updates the View using the Model.
     *
     * @param s Current state
     */
    return (s: State) => {
        // Scroll two repeated scenery panels and wrap them seamlessly.
        const cloudOffset = (s.time * Constants.CLOUD_SPEED) % Viewport.CANVAS_WIDTH;
        const groundOffset = (s.time * Constants.GROUND_SPEED) % Viewport.CANVAS_WIDTH;
        cloudScroller.setAttribute("transform", `translate(${-cloudOffset} 0)`);
        groundScroller.setAttribute("transform", `translate(${-groundOffset} 0)`);
        
        // Remove all existing pipe elements from the SVG
        svg.querySelectorAll(".pipe").forEach(pipe => pipe.remove());

        s.pipes.forEach(pipe => {
            // Top pipe
            const pipeTop = createSvgElement(svg.namespaceURI, "rect", {
                class: "pipe",
                x: `${pipe.x}`,
                y: `0`,
                width: `${Constants.PIPE_WIDTH}`,
                height: `${pipe.gap_y - pipe.gap_height / 2}`,
                fill: "green",
            });
            // Bottom pipe
            const pipeBottom = createSvgElement(svg.namespaceURI, "rect", {
                class: "pipe",
                x: `${pipe.x}`,
                y: `${pipe.gap_y + pipe.gap_height / 2}`,
                width: `${Constants.PIPE_WIDTH}`,
                height: `${Viewport.CANVAS_HEIGHT - (pipe.gap_y + pipe.gap_height / 2)}`,
                fill: "green",
            });
            svg.appendChild(pipeTop);
            svg.appendChild(pipeBottom);
        });

        svg.querySelectorAll(".heart").forEach(heart => heart.remove());
        s.hearts.forEach(heart => {
            const heartShape = createSvgElement(svg.namespaceURI, "path", {
                class: "heart",
                d: "M9 16.4C7.8 15.2 1 10.3 1 5.5C1 2.6 3.2 1 5.5 1C7 1 8.3 1.8 9 3C9.7 1.8 11 1 12.5 1C14.8 1 17 2.6 17 5.5C17 10.3 10.2 15.2 9 16.4Z",
                transform: `translate(${heart.x} ${heartY(heart, s.time)})`,
                role: "img",
                "aria-label": "Extra life heart",
            });
            svg.appendChild(heartShape);
        });

        // Update bee position
        beeImg.setAttribute("y", `${s.bee.y}`);
        bringToForeground(beeImg);

        // Update lives and score text
        livesText.innerText =  `${s.lives}`;
        scoreText.innerText = `${s.score}`;

        /** Show pause box if game ends, else hide */
        (s.paused ? show : hide)(pause);

        /** Show game over box if game ends, else hide */
        s.gameEnd 
        ? (show(gameOver), bringToForeground(gameOver)) 
        : hide(gameOver);

        /**
         * Renders "ghost bees" that represent the player's previous flight paths.
         * - A ghost bee is created once per recorded path (if it doesn't already exist).
         * - Each ghost bee replays its path, synchronized with the current time step.
         * - Ghosts fade out (hidden) when the game ends or the path is complete.
         *
         * This helps visualize past attempts alongside the current run.
         */

        // Check if the number of ghost bees matches number of paths
        if (svg.querySelectorAll(".ghost").length < s.previousPaths.length) {
            s.previousPaths.forEach((_, i) => {
                // Create ghost if doesnt exist yet
                if (!svg.querySelector(`#ghost-${i}`)) {
                    svg.appendChild(
                    createSvgElement(svg.namespaceURI, "image", {
                        id: `ghost-${i}`,
                        class: "ghost",
                        href: "assets/bee.png",
                        x: `${s.bee.x}`,
                        y: `${initialState.bee.y}`,
                        width: `${Bee.WIDTH}`,
                        height: `${Bee.HEIGHT}`,
                        style: "opacity: 0.3;",
                        })
                    );
                }
            });
          }
          // Determine the current step in the ghost timeline 
          const step = Math.floor(s.time / (Constants.TICK_RATE_MS / 1000));

          // Update each ghost along its path
          s.previousPaths.forEach((path, i) => {
            const ghost = svg.querySelector(`#ghost-${i}`) as SVGElement;
            if (!ghost) return;
          
            // Hide ghost if game eneded or past its length
            if (s.gameEnd || step >= path.length) {
              hide(ghost);
            } else {
                // Else show and update the ghost
                ghost.setAttribute("visibility", "visible");
                ghost.setAttribute("y", `${path[step]}`);
            }
          });
    };
};

/** ---------------- Game state$ Stream --------------- */
 
/**
 * Creates the main game state observable.
 * 
 * @param csvContents containing pipes
 * @returns Observable<State> that emits the current game
 */
export const state$ = (csvContents: string): Observable<State> => {

    /** User input */
    const key$ = fromEvent<KeyboardEvent>(document, "keydown");
    const fromKey = (keyCode: Key) =>
        key$.pipe(filter(({ code }) => code === keyCode));


    /** Pause/resume mechanism using Escape key */
    const pause$ = fromKey("Escape").pipe(
        // Toggle true/false each time Esc is pressed
        scan((paused) => !paused, false),
        startWith(false), // initially not paused
    ); 

    /** Reducer from pause$ to updates s.pause */
    const pauseReducer$ = pause$.pipe(
        map(paused => (s: State): State => ({
        ...s,
        paused: paused
        }))
    );

    /** Pipes steam */
    const pipes$: (
        pipesFromCSV: readonly Pipe[]) => 
            Observable<(s: State) => State> = (pipesFromCSV) =>

                interval(Constants.TICK_RATE_MS).pipe(

                    withLatestFrom(pause$),              // get current pause state
                    filter(([_, paused]) => !paused),   // continue if not paused

                    // Map each tick into state function
                    map(() => (s: State): State => {
                        if (s.gameEnd) return s; 
                        
                        const newTime = s.time + Constants.TICK_RATE_MS / 1000;
                        // Update and move all pipes following newTime
                        const newPipes = updatePipes(
                            s.pipes, 
                            pipesFromCSV, 
                            s.time, 
                            newTime)
                        return {
                        ...s,
                        pipes: newPipes
                        };
                    })
                );


    /** Pipes from CSV */
    const pipesFromCSV = parseCSV(csvContents);

    /** Stream of pipe states for the game  */
    const pipesStream$ = pipes$(pipesFromCSV);

    /** Determines the rate of time steps */
    const tick$: Observable<(s: State) => State> = 
    interval(Constants.TICK_RATE_MS).pipe(
        withLatestFrom(pause$),
        filter(([_, paused]) => !paused),
        map(() => (s: State): State => 
            s.gameEnd ? s : tick(pipesFromCSV)(s))
    );

    /** Jump mechanism */
    const jump$: Observable<(s: State) => State> = fromKey("Space").pipe(
        withLatestFrom(pause$),              // get current pause state
        filter(([_, paused]) => !paused),   // continue if not paused
        map(() => (s: State): State =>
          s.gameEnd
            ? s
            : {
                ...s,
                bee: {
                  ...s.bee,
                  y: Math.max(Constants.CEILING, s.bee.y - 10),
                  velocity: -5,
                },
              }
        ),
      );

      /** Restart mechanism */
      const restart$: Observable<(s: State) => State> = fromKey("KeyR").pipe(
        withLatestFrom(pause$),              // get current pause state
        filter(([_, paused]) => !paused),   // continue if not paused
        map(() => (s: State): State =>
            s.gameEnd
                ? {
                    ...initialState,
                    previousPaths: [...s.previousPaths, [...s.currentPath]],
                }
                : s // Else continue gameplay
            )
      );

    // Return observables
    return merge(tick$, jump$, restart$, pipesStream$, pauseReducer$).pipe(
        scan((currentState, updateFn) => updateFn(currentState), initialState)
    );
};

/** ---------------- MAIN --------------- */

// The following simply runs your main function on window load.  Make sure to leave it in place.
// You should not need to change this, beware if you are.
if (typeof window !== "undefined") {
    const { protocol, hostname, port } = new URL(import.meta.url);
    const baseUrl = `${protocol}//${hostname}${port ? `:${port}` : ""}`;
    const csvUrl = `${baseUrl}/assets/map.csv`;

    // Get the file from URL
    const csv$ = fromFetch(csvUrl).pipe(
        switchMap(response => {
            if (response.ok) {
                return response.text();
            } else {
                throw new Error(`Fetch error: ${response.status}`);
            }
        }),
        catchError(err => {
            console.error("Error fetching the CSV file:", err);
            throw err;
        }),
    );

    // Observable: wait for first user click
    const click$ = fromEvent(document.body, "mousedown").pipe(take(1));

    csv$.pipe(
        switchMap(contents =>
            // On click - start the game
            click$.pipe(switchMap(() => state$(contents))),
        ),
    ).subscribe(render());
}
